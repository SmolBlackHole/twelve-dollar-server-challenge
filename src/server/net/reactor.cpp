#include "reactor.hpp"
#include "../diagnostics/io.hpp"

#include <algorithm>
#include <arpa/inet.h>
#include <cassert>
#include <cerrno>
#include <exception>
#include <netinet/in.h>
#include <poll.h>
#include <stdexcept>
#include <string>
#include <sys/eventfd.h>
#include <sys/resource.h>
#include <system_error>
#include <unistd.h>
#include <utility>

namespace tds::net
{
using telemetry::IoTelemetry;

Reactor::Reactor(std::string_view host, std::uint16_t port)
    : listener_(::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0)),
      stop_(::eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK))
{
    int enabled = 1;
    if (::setsockopt(listener_.get(), SOL_SOCKET, SO_REUSEADDR, &enabled, sizeof(enabled)) < 0)
    {
        throw std::system_error(errno, std::generic_category(), "reuse address");
    }
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(port);
    const std::string host_string(host);
    if (::inet_pton(AF_INET, host_string.c_str(), &address.sin_addr) != 1)
    {
        throw std::runtime_error("HOST must be an IPv4 address");
    }
    if (::bind(listener_.get(), reinterpret_cast<const sockaddr *>(&address), sizeof(address)) <
            0 ||
        ::listen(listener_.get(), 4096) < 0)
    {
        throw std::system_error(errno, std::generic_category(), "listen");
    }

    socklen_t length = sizeof(address);
    if (::getsockname(listener_.get(), reinterpret_cast<sockaddr *>(&address), &length) < 0)
    {
        throw std::system_error(errno, std::generic_category(), "socket address");
    }
    port_ = ntohs(address.sin_port);
    rlimit limit{};
    if (::getrlimit(RLIMIT_NOFILE, &limit) < 0)
    {
        throw std::system_error(errno, std::generic_category(), "file limit");
    }
    table_size_ = static_cast<unsigned>(std::min<rlim_t>(limit.rlim_cur, 65535));
    if (!table_size_)
    {
        throw std::runtime_error("file limit leaves no direct descriptor slots");
    }
    rings_.push_back(std::make_unique<Ring>(table_size_, true));
}

Reactor::~Reactor()
{
    // The primary owns wake READs into satellites, so stop it before their storage dies
    rings_.front().reset();
}

void Reactor::start()
{
    accept();
    io_uring_prep_poll_add(rings_.front()->entry(OperationId::control(Event::stop)), stop_.get(),
                           POLLIN);
}

unsigned Reactor::pending_count() const noexcept
{
    unsigned count = 0;
    for (const auto &owner : rings_)
    {
        count += owner->pending;
    }

    return count;
}

bool Reactor::has_work() const noexcept
{
    for (const auto &owner : rings_)
    {
        if (owner->pending || owner->staged_count || !owner->ready.empty() ||
            !owner->deferred_receives.empty() || (!stopping_ && owner->accept_refill_needed) ||
            (stopping_ && owner->shutdown_cursor < owner->connections.size()))
        {
            return true;
        }
    }

    return false;
}

void Reactor::begin_turn()
{
    bool refill = false;
    for (const auto &owner : rings_)
    {
        owner->work_visits = 0;
        owner->rx_slices = 0;
        if (owner->accept_refill_needed)
        {
            owner->accept_refill_needed = false;
            refill = true;
        }
    }
    if (refill)
    {
        accept();
    }
    int result;
    do
    {
        result = wait();
    } while (result == -EINTR);
    if (result < 0 && result != -ETIME && result != -EBUSY)
    {
        throw std::system_error(-result, std::generic_category(), "wait");
    }
    turn_count_ = 0;
    accepted_ = false;
    ring_index_ = 0;
    ring_started_ = false;
}

bool Reactor::next(Completion &completion)
{
    completion = Completion{};
    while (ring_index_ < rings_.size())
    {
        auto &owner = *rings_[ring_index_];
        if (!ring_started_)
        {
            begin_ring(owner);
        }
        if (cq_cursor_.head == cq_cursor_.tail)
        {
            finish_ring();
            continue;
        }

        consume(owner, completion);

        return true;
    }

    return false;
}

void Reactor::begin_ring(Ring &owner)
{
    IoTelemetry::drain_started(owner);
    // Ring never enables CQE32 or mixed CQEs, so each completion advances one entry
    auto &cq = owner.queue.cq;
    const auto head = *cq.khead;
    const auto ready = io_uring_smp_load_acquire(cq.ktail) - head;
    cq_cursor_ = {cq.cqes, cq.ring_mask, head, head + std::min(ready, 256U)};
    drained_ = 0;
    ring_started_ = true;
}

void Reactor::consume(Ring &owner, Completion &completion)
{
    auto *cqe = &cq_cursor_.cqes[cq_cursor_.head++ & cq_cursor_.mask];
    // MORE publishes another result without retiring the original SQE
    if (!(cqe->flags & IORING_CQE_F_MORE))
    {
        assert(owner.pending);
        --owner.pending;
    }
    const auto operation = OperationId::from_raw(io_uring_cqe_get_data64(cqe));
    ++drained_;
    complete(owner, operation, cqe->res, cqe->flags, completion);
}

std::span<Completion> Reactor::collect()
{
    while (ring_index_ < rings_.size())
    {
        auto &owner = *rings_[ring_index_];
        if (!ring_started_)
        {
            begin_ring(owner);
        }
        for (unsigned index = 0; index < owner.completion_count; ++index)
        {
            owner.completions[index].reset();
        }
        owner.completion_count = 0;
        // Reserve one possible new work ticket for every collected record
        const auto limit = static_cast<unsigned>(owner.ready.remaining());
        while (cq_cursor_.head != cq_cursor_.tail && owner.completion_count < limit)
        {
            auto &record = owner.completions[owner.completion_count++];
            record.stage(owner);
            consume(owner, record);
        }
        finish_ring();
        if (owner.completion_count)
        {
            return std::span(owner.completions).first(owner.completion_count);
        }
    }

    return {};
}

bool Reactor::schedule(Connection &connection, WorkReason reason) noexcept
{
    connection.work_reasons |= reason;
    if (connection.work_queued || connection.work_active)
    {
        return true;
    }
    if (!connection.ring.ready.push(connection.id()))
    {
        return false;
    }
    connection.work_queued = true;

    return true;
}

void Reactor::finish_ring()
{
    auto &owner = *rings_[ring_index_];
    // Every consumed CQE has an owning result before its slot is returned to the kernel
    if (drained_)
    {
        io_uring_smp_store_release(owner.queue.cq.khead, cq_cursor_.head);
        turn_count_ += drained_;
    }
    IoTelemetry::drain_finished(owner, drained_);
    if (stopping_)
    {
        advance_shutdown(owner);
    }
    ring_started_ = false;
    ++ring_index_;
}

void Reactor::finish_turn()
{
    assert(!ring_started_ && ring_index_ == rings_.size());
    if (!stopping_)
    {
        // Collection hands off leases; consume their returned credits before waiting again
        for (const auto &owner : rings_)
        {
            retry_receives(*owner);
        }
    }
    finish_wakeups();
    previous_count_ = turn_count_;
}

bool Reactor::stopping() const noexcept
{
    return stopping_;
}

void Reactor::receive(Connection &connection)
{
    receive(connection, true);
}

void Reactor::receive(Connection &connection, bool poll_first)
{
    if (connection.closing() || connection.rx_eof || connection.receive_deferred)
    {
        throw std::logic_error("receive has no eligible connection owner");
    }
    connection.rx.start();
    auto *request = connection.ring.entry(OperationId::connection(Event::receive, connection.id()));
    // Cursed compatibility detail: 6.8 needs zero length with selected multishot buffers
    io_uring_prep_recv_multishot(request, static_cast<int>(connection.slot.value), nullptr, 0, 0);
    request->flags |= IOSQE_BUFFER_SELECT | IOSQE_FIXED_FILE;
    request->buf_group = ReceiveBuffers::group;
    if (poll_first)
    {
        request->ioprio |= IORING_RECVSEND_POLL_FIRST;
    }
    IoTelemetry::receive_prepared(connection);
}

void Reactor::send(Connection &connection, msghdr &message)
{
    if (connection.closing())
    {
        throw std::logic_error("send has no live connection owner");
    }
    connection.tx.start();
    auto *request = connection.ring.entry(OperationId::connection(Event::send, connection.id()));
    io_uring_prep_sendmsg(request, static_cast<int>(connection.slot.value), &message, MSG_NOSIGNAL);
    request->flags |= IOSQE_FIXED_FILE;
}

void Reactor::close(Connection &connection)
{
    if (connection.closing())
    {
        throw std::logic_error("duplicate connection close");
    }
    connection.close_phase = ClosePhase::requested;
    connection.receive_deferred = false;
    connection.rx_paused = false;
    connection.work_reasons = WorkReason::none;
    connection.ring.backlog.clear(connection.input);
    cancel_receive(connection);
    cancel_send(connection);
    advance_close(connection);
}

void Reactor::cancel_receive(Connection &connection)
{
    if (connection.rx.request_cancel())
    {
        cancel(connection.ring, OperationId::connection(Event::receive, connection.id()),
               OperationId::connection(Event::cancel_receive, connection.id()));
    }
}

void Reactor::cancel_send(Connection &connection)
{
    if (connection.tx.request_cancel())
    {
        cancel(connection.ring, OperationId::connection(Event::send, connection.id()),
               OperationId::connection(Event::cancel_send, connection.id()));
    }
}

void Reactor::resume_receive(Connection &connection)
{
    if (!connection.rx_paused || connection.rx.busy() || connection.rx_eof ||
        connection.closing() || connection.input.count || connection.receive_deferred)
    {
        return;
    }
    auto &owner = connection.ring;
    if (!owner.receive_buffers->take_retry_credits(1))
    {
        const auto now = std::chrono::steady_clock::now();
        if (now < owner.next_receive_probe)
        {
            connection.receive_deferred = true;
            owner.deferred_receives.push_back(connection.id());

            return;
        }
        owner.next_receive_probe = now + std::chrono::milliseconds(4);
    }
    connection.rx_paused = false;
    receive(connection);
}

void Reactor::advance_close(Connection &connection)
{
    if (connection.close_phase != ClosePhase::requested || connection.rx.busy() ||
        connection.tx.busy())
    {
        return;
    }
    connection.close_phase = ClosePhase::shutdown;
    // Cursed kernel detail: unregistering a fixed slot can defer socket release behind other I/O
    // Send TCP EOF before close_direct drops the registration
    auto *request =
        connection.ring.entry(OperationId::connection(Event::shutdown, connection.id()));
    io_uring_prep_shutdown(request, static_cast<int>(connection.slot.value), SHUT_RDWR);
    request->flags |= IOSQE_FIXED_FILE;
}

void Reactor::request_stop() const noexcept
{
    const std::uint64_t value = 1;
    ssize_t result;
    do
    {
        result = ::write(stop_.get(), &value, sizeof(value));
    } while (result < 0 && errno == EINTR);
    if (result < 0 && errno != EAGAIN)
    {
        std::terminate();
    }
}

int Reactor::stop_descriptor() const noexcept
{
    return stop_.get();
}

std::uint16_t Reactor::port() const noexcept
{
    return port_;
}

std::span<const std::unique_ptr<Ring>> Reactor::rings() const noexcept
{
    return rings_;
}

void Reactor::accept()
{
    if (stopping_)
    {
        return;
    }
    for (unsigned index = 0; index < accept_tickets_.size(); ++index)
    {
        auto &ticket = accept_tickets_[index];
        if (ticket.pending || ticket.cancelling)
        {
            continue;
        }
        Ring *accepting = nullptr;
        for (const auto &owner : rings_)
        {
            if (owner->connections.has_free_slot())
            {
                accepting = owner.get();
                break;
            }
        }
        if (!accepting)
        {
            if (rings_.size() == max_rings)
            {
                return;
            }
            auto owner =
                std::make_unique<Ring>(table_size_, false, static_cast<unsigned>(rings_.size()));
            accepting = owner.get();
            rings_.push_back(std::move(owner));
            arm_wake(*accepting);
        }
        ticket.ring = accepting;
        ticket.slot = accepting->connections.reserve();
        ticket.pending = true;
        io_uring_prep_accept_direct(accepting->entry(OperationId::control(Event::accept, index)),
                                    listener_.get(), nullptr, nullptr, SOCK_NONBLOCK,
                                    ticket.slot.value);
    }
}

void Reactor::arm_wake(Ring &owner)
{
    // Let the pending read sleep in io_uring instead of polling and issuing another syscall
    owner.wake_counter = 0;
    io_uring_prep_read(rings_.front()->entry(OperationId::control(Event::wake, owner.index)),
                       owner.notification->get(), &owner.wake_counter, sizeof(owner.wake_counter),
                       0);
    owner.wake_pending = true;
}

void Reactor::retry_receives(Ring &owner)
{
    if (owner.deferred_receives.empty())
    {
        return;
    }
    const auto now = std::chrono::steady_clock::now();
    for (unsigned scanned = 0; scanned < 256 && !owner.deferred_receives.empty(); ++scanned)
    {
        const auto key = owner.deferred_receives.front();
        auto *connection = owner.connections.find(key);
        if (!connection || !connection->receive_deferred || connection->closing() ||
            connection->rx_eof || connection->rx.busy() ||
            (connection->rx_paused && connection->input.count))
        {
            owner.deferred_receives.pop_front();
            if (connection)
            {
                connection->receive_deferred = false;
            }
            continue;
        }
        if (!owner.receive_buffers->take_retry_credits(1) && now < owner.next_receive_probe)
        {
            break;
        }
        // Stale queue entries spend no credit; a quiet ring gets one timed probe
        owner.next_receive_probe = now + std::chrono::milliseconds(4);
        owner.deferred_receives.pop_front();
        connection->receive_deferred = false;
        connection->rx_paused = false;
        // Try deferred receives before arming another poll; empty sockets use normal fallback
        receive(*connection, false);
    }
}

void Reactor::cancel(Ring &owner, OperationId original, OperationId acknowledgement)
{
    io_uring_prep_cancel64(owner.entry(acknowledgement), original.raw(), 0);
}

void Reactor::stop_all()
{
    stopping_ = true;
    for (unsigned index = 0; index < accept_tickets_.size(); ++index)
    {
        auto &ticket = accept_tickets_[index];
        if (ticket.pending)
        {
            ticket.cancelling = true;
            cancel(*ticket.ring, OperationId::control(Event::accept, index),
                   OperationId::control(Event::cancel_accept, index));
        }
    }
    for (const auto &owner : rings_)
    {
        owner->deferred_receives.clear();
    }
}

void Reactor::advance_shutdown(Ring &owner)
{
    // Bound cancellation work so an idle population cannot fill the CQ before its next drain
    unsigned count = 0;
    while (owner.shutdown_cursor < owner.connections.size() && count < 128)
    {
        auto *connection = owner.connections.at(owner.shutdown_cursor++);
        if (!connection)
        {
            continue;
        }
        ++count;
        if (!connection->closing() && connection->request_phase != RequestPhase::queued_write)
        {
            close(*connection);
        }
    }
}

void Reactor::complete_wake(Ring &owner, int result)
{
    owner.wake_pending = false;
    if (result >= 0 && result != static_cast<int>(sizeof(owner.wake_counter)))
    {
        throw std::runtime_error("short ring notification read");
    }
    if (result < 0 && result != -ECANCELED && result != -EINTR)
    {
        throw std::system_error(-result, std::generic_category(), "ring notification read");
    }
    if (!closing_wakeups_)
    {
        arm_wake(owner);
    }
}

void Reactor::complete_accept(AcceptTicket &ticket, int result)
{
    accepted_ = true;
    auto &owner = *ticket.ring;
    ticket.pending = false;
    if (result >= 0)
    {
        auto &connection = owner.connections.create(owner, ticket.slot);
        if (stopping_)
        {
            close(connection);
        }
        else
        {
            receive(connection);
        }
    }
    else
    {
        owner.connections.release(ticket.slot);
        if (result != -ECANCELED && result != -EINTR && result != -EAGAIN && result != -EMFILE &&
            result != -ENFILE)
        {
            throw std::system_error(-result, std::generic_category(), "accept");
        }
    }
    accept();
}

void Reactor::complete_shutdown(Connection &connection, int result)
{
    // A reset or already disconnected peer still needs its fixed slot released
    if (result < 0 && result != -ENOTCONN)
    {
        throw std::system_error(-result, std::generic_category(), "shutdown direct socket");
    }
    if (connection.close_phase != ClosePhase::shutdown)
    {
        throw std::logic_error("shutdown completion has no live owner");
    }
    connection.close_phase = ClosePhase::close;
    io_uring_prep_close_direct(
        connection.ring.entry(OperationId::connection(Event::close, connection.id())),
        connection.slot.value);
}

void Reactor::complete_close(Ring &owner, Connection &connection, int result)
{
    if (result < 0)
    {
        throw std::system_error(-result, std::generic_category(), "close direct descriptor");
    }
    if (connection.close_phase != ClosePhase::close)
    {
        throw std::logic_error("close completion has no live owner");
    }
    connection.close_phase = ClosePhase::complete;
    owner.try_retire(connection.id());
}

void Reactor::complete_io(Connection &connection, Event event, int result, unsigned flags,
                          ReceiveBufferLease buffer, Completion &completion)
{
    const bool more = (flags & IORING_CQE_F_MORE) != 0;
    if (more && event != Event::receive)
    {
        throw std::runtime_error("unexpected persistent I/O completion");
    }
    auto &state = event == Event::receive ? connection.rx : connection.tx;
    state.complete(more);
    if (event == Event::send)
    {
        IoTelemetry::send_serviced(completion);
    }
    else
    {
        IoTelemetry::receive_completed(connection, result, buffer);
    }
    if (event == Event::send)
    {
        completion.kind = CompletionKind::sent;
        completion.connection = &connection;
        completion.result = result;
    }
    if (stopping_ || connection.closing())
    {
        if (!connection.closing())
        {
            close(connection);
        }
        else
        {
            advance_close(connection);
        }

        return;
    }
    // Cancel ACKs and the original terminal can arrive in either order
    // A paused receive stays paused until its owner explicitly rearms it
    if (event == Event::receive && result == -ECANCELED && state.cancel_requested())
    {
        return;
    }
    if (event == Event::receive && result == -ENOBUFS)
    {
        if (!state.cancel_requested())
        {
            IoTelemetry::receive_deferred(connection);
            connection.receive_deferred = true;
            connection.ring.deferred_receives.push_back(connection.id());
        }

        return;
    }
    if (event == Event::receive && result == 0)
    {
        connection.rx_eof = true;
        completion.kind = CompletionKind::received;
        completion.buffer = std::move(buffer);

        return;
    }
    if (result == 0 || (result < 0 && result != -EAGAIN && result != -EINTR))
    {
        close(connection);

        return;
    }
    if (result < 0)
    {
        if (event == Event::receive && !state.cancel_requested())
        {
            receive(connection);
        }

        return;
    }
    if (event == Event::receive)
    {
        if (!(flags & IORING_CQE_F_BUFFER) || static_cast<unsigned>(result) > ReceiveBuffers::size)
        {
            throw std::runtime_error("invalid receive buffer result");
        }
        completion.kind = CompletionKind::received;
        // A successful final result still ends multishot and needs a fresh receive later
        if (!more)
        {
            connection.rx_paused = true;
        }
        completion.connection = &connection;
        completion.result = result;
        completion.buffer = std::move(buffer);
    }
}

void Reactor::complete(Ring &owner, OperationId operation, int result, unsigned flags,
                       Completion &completion)
{
    completion.operation = operation;
    completion.source = &owner;
    completion.flags = flags;
    completion.result = result;
    const bool selected = (flags & IORING_CQE_F_BUFFER) != 0;
    const auto id = flags >> IORING_CQE_BUFFER_SHIFT;
    if (selected &&
        (id >= ReceiveBuffers::count || result > static_cast<int>(ReceiveBuffers::size)))
    {
        throw std::runtime_error("invalid receive buffer id");
    }
    const auto length = result > 0 ? static_cast<std::size_t>(result) : 0;
    ReceiveBufferLease buffer(selected ? owner.receive_buffers.get() : nullptr,
                              static_cast<unsigned short>(id), length);
    const auto event = operation.event();
    if (operation.is_connection())
    {
        auto *connection = owner.connections.find(operation.connection_key());
        if (!connection)
        {
            // Stale identities still owe the provided-buffer ring its selected lease
            return;
        }
        completion.hold(*connection);
        switch (event)
        {
        case Event::receive:
        case Event::send:
            complete_io(*connection, event, result, flags, std::move(buffer), completion);
            return;
        case Event::cancel_receive:
        case Event::cancel_send:
            if (result < 0 && result != -ENOENT && result != -EALREADY)
            {
                throw std::system_error(-result, std::generic_category(), "cancel connection I/O");
            }
            (event == Event::cancel_receive ? connection->rx : connection->tx).acknowledge_cancel();
            advance_close(*connection);
            return;
        case Event::shutdown:
            complete_shutdown(*connection, result);
            return;
        case Event::close:
            complete_close(owner, *connection, result);
            return;
        default:
            break;
        }
    }
    if (operation.generation() || (flags & IORING_CQE_F_MORE))
    {
        throw std::runtime_error("invalid control operation identity");
    }
    switch (event)
    {
    case Event::none:
        return;
    case Event::stop:
        stop_all();
        return;
    case Event::wake:
        complete_wake(*rings_.at(operation.index()), result);
        return;
    case Event::accept:
        complete_accept(accept_tickets_.at(operation.index()), result);
        return;
    case Event::cancel_accept:
    case Event::cancel_wake:
        if (result < 0 && result != -ENOENT && result != -EALREADY)
        {
            throw std::system_error(-result, std::generic_category(), "cancel control operation");
        }
        if (event == Event::cancel_accept)
        {
            accept_tickets_.at(operation.index()).cancelling = false;
        }
        else
        {
            rings_.at(operation.index())->wake_cancelling = false;
        }
        return;
    default:
        throw std::runtime_error("unknown completion operation identity");
    }
}

int Reactor::wait()
{
    // Accept handling can grow rings, but it never runs while wait submits or polls
    const auto owners = rings();
    auto &primary = owners.front()->queue;
    for (std::size_t index = 0; index < owners.size(); ++index)
    {
        const auto &owner = owners[index];
        if (index)
        {
            owner->submit();
        }
    }
    for (const auto &owner : owners)
    {
        if (!owner->ready.empty() || owner->staged_count ||
            (stopping_ && owner->shutdown_cursor < owner->connections.size()))
        {
            return io_uring_submit(&primary);
        }
    }
    // Cursed: an empty SQ does not mean the primary gets a day off
    // TASKRUN and overflow flags make submit enter GETEVENTS when progress is needed
    for (std::size_t index = 1; index < owners.size(); ++index)
    {
        if (io_uring_cq_ready(&owners[index]->queue))
        {
            return io_uring_submit(&primary);
        }
    }
    const bool immediate = accepted_ || previous_count_ < 2;
    const auto has_deferred_receives = [&owners] {
        for (const auto &owner : owners)
        {
            if (!owner->deferred_receives.empty())
            {
                return true;
            }
        }

        return false;
    };
    // A quiet turn can block only when no deferred receive needs a timed probe
    if (stopping_ || !(primary.features & IORING_FEAT_EXT_ARG) ||
        (immediate && !has_deferred_receives()))
    {
        return io_uring_submit_and_wait(&primary, 1);
    }
    // Exhausted receive queues keep their 4-ms probe even while busy turns collect longer
    const auto wait_ns = !immediate && !has_deferred_receives() ? 8'000'000 : 4'000'000;
    __kernel_timespec timeout{0, wait_ns};
    io_uring_cqe *first = nullptr;

    // Keep sparse retry turns prompt; busy turns collect CQEs for larger write batches
    return io_uring_submit_and_wait_timeout(&primary, &first, immediate ? 1 : 128, &timeout,
                                            nullptr);
}

void Reactor::finish_wakeups()
{
    if (!stopping_ || closing_wakeups_)
    {
        return;
    }
    unsigned polls = 0;
    for (const auto &owner : rings_)
    {
        if (owner->shutdown_cursor != owner->connections.size() || !owner->ready.empty() ||
            owner->staged_count)
        {
            return;
        }
        polls += static_cast<unsigned>(owner->wake_pending);
    }
    if (pending_count() != polls)
    {
        return;
    }
    // Keep wake targets alive until every original operation and close CQE has drained
    closing_wakeups_ = true;
    for (const auto &owner : rings_)
    {
        if (owner->wake_pending)
        {
            owner->wake_cancelling = true;
            cancel(*rings_.front(), OperationId::control(Event::wake, owner->index),
                   OperationId::control(Event::cancel_wake, owner->index));
        }
    }
}

} // namespace tds::net
