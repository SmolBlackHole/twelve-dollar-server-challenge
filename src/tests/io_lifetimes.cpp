#include "diagnostics/io.hpp"
#include "http/exchange.hpp"
#include "net/reactor.hpp"

#include <yyjson.h>

#include <algorithm>
#include <arpa/inet.h>
#include <array>
#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <exception>
#include <memory>
#include <optional>
#include <poll.h>
#include <stdexcept>
#include <string>
#include <string_view>
#include <sys/resource.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <thread>
#include <unistd.h>
#include <utility>
#include <vector>

namespace
{
using namespace std::chrono_literals;
using Clock = std::chrono::steady_clock;
using tds::net::ClosePhase;
using tds::net::Completion;
using tds::net::CompletionKind;
using tds::net::Connection;
using tds::net::Event;
using tds::net::OperationId;
using tds::net::Reactor;
using tds::net::ReceiveBufferLease;
using tds::net::ReceiveBuffers;
using tds::net::Ring;

void require(bool condition, const char *message)
{
    if (!condition)
    {
        throw std::runtime_error(message);
    }
}

template <class Action> void rejects(Action &&action, const char *message)
{
    try
    {
        std::forward<Action>(action)();
    }
    catch (const std::logic_error &)
    {
        return;
    }

    throw std::runtime_error(message);
}

class FileLimit
{
  public:
    explicit FileLimit(rlim_t soft)
    {
        require(::getrlimit(RLIMIT_NOFILE, &previous_) == 0, "read descriptor limit");
        require(previous_.rlim_cur >= soft, "descriptor limit too low for fixture");
        const rlimit limited{soft, previous_.rlim_max};
        require(::setrlimit(RLIMIT_NOFILE, &limited) == 0, "set fixture descriptor limit");
    }

    ~FileLimit()
    {
        ::setrlimit(RLIMIT_NOFILE, &previous_);
    }

    FileLimit(const FileLimit &) = delete;
    FileLimit &operator=(const FileLimit &) = delete;

  private:
    rlimit previous_{};
};

class Client
{
  public:
    explicit Client(std::uint16_t port, int receive_buffer = 0)
        : descriptor_(::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0))
    {
        require(descriptor_ >= 0, "client socket failed");
        if (receive_buffer && ::setsockopt(descriptor_, SOL_SOCKET, SO_RCVBUF, &receive_buffer,
                                           sizeof(receive_buffer)) < 0)
        {
            ::close(descriptor_);
            descriptor_ = -1;
            throw std::runtime_error("client receive buffer failed");
        }
        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_port = htons(port);
        address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        const int connected =
            ::connect(descriptor_, reinterpret_cast<const sockaddr *>(&address), sizeof(address));
        if (connected == 0)
        {
            return;
        }
        if (errno == EINPROGRESS)
        {
            pollfd pending{descriptor_, POLLOUT, 0};
            int error = 0;
            socklen_t length = sizeof(error);
            if (::poll(&pending, 1, 1000) > 0 &&
                ::getsockopt(descriptor_, SOL_SOCKET, SO_ERROR, &error, &length) == 0 && !error)
            {
                return;
            }
        }
        ::close(descriptor_);
        descriptor_ = -1;

        throw std::runtime_error("client connect failed");
    }

    ~Client()
    {
        ::close(descriptor_);
    }

    Client(const Client &) = delete;
    Client &operator=(const Client &) = delete;

    void send_byte() const
    {
        require(::send(descriptor_, "x", 1, MSG_NOSIGNAL) == 1, "client send failed");
    }

    void finish_writing() const
    {
        require(::shutdown(descriptor_, SHUT_WR) == 0, "client half-close failed");
    }

    std::size_t drain_bytes(char expected) const
    {
        std::array<char, 4096> buffer;
        std::size_t total = 0;
        while (true)
        {
            const auto result = ::recv(descriptor_, buffer.data(), buffer.size(), MSG_DONTWAIT);
            if (result < 0 && errno == EINTR)
            {
                continue;
            }
            if (result < 0 && (errno == EAGAIN || errno == EWOULDBLOCK))
            {
                return total;
            }
            require(result > 0, "primary send closed or failed before all bytes arrived");
            const auto count = static_cast<std::size_t>(result);
            for (std::size_t index = 0; index < count; ++index)
            {
                require(buffer[index] == expected, "primary send corrupted its payload");
            }
            total += count;
        }
    }

    bool closed(Clock::time_point deadline) const
    {
        while (Clock::now() < deadline)
        {
            char byte = 0;
            const auto result = ::recv(descriptor_, &byte, 1, MSG_DONTWAIT);
            if (result == 0)
            {
                return true;
            }
            if (result > 0 || (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR))
            {
                return false;
            }
            pollfd pending{descriptor_, POLLIN | POLLHUP, 0};
            ::poll(&pending, 1, 1);
        }

        return false;
    }

  private:
    int descriptor_;
};

void await_completion(Reactor &reactor, Clock::time_point deadline)
{
    for (const auto &ring : reactor.rings())
    {
        ring->submit();
    }
    while (Clock::now() < deadline)
    {
        for (const auto &ring : reactor.rings())
        {
            const int result = io_uring_get_events(&ring->queue);
            require(result >= 0 || result == -EINTR, "get events failed");
            if (io_uring_cq_ready(&ring->queue))
            {
                return;
            }
        }
        std::this_thread::sleep_for(1ms);
    }

    throw std::runtime_error("waiting for a CQE timed out");
}

template <class Handler> void drain_turn(Reactor &reactor, Handler &&handler)
{
    Completion completion;
    while (reactor.next(completion))
    {
        handler(completion);
    }
    reactor.finish_turn();
}

Connection &only_connection(Reactor &reactor)
{
    Connection *found = nullptr;
    for (const auto &ring : reactor.rings())
    {
        for (std::size_t slot = 0; slot < ring->connections.size(); ++slot)
        {
            if (auto *connection = ring->connections.at(slot))
            {
                require(!found, "more than one initial connection");
                found = connection;
            }
        }
    }
    require(found != nullptr, "initial client was not accepted");

    return *found;
}

void reservation_and_record_guards()
{
    Ring owner(3, true);
    auto &store = owner.connections;
    const auto first = store.reserve();
    const auto second = store.reserve();
    const auto spare = store.reserve();
    require(!store.has_free_slot(), "reserved slots remained free");
#if TDS_TELEMETRY_LEVEL >= 1
    require(store.live_count() == 0 && store.reserved_count() == 3,
            "reservation gauges lost an unaccepted slot");
#endif
    rejects(
        [&] {
            store.reserve();
        },
        "exhausted store returned another slot");

    auto &record = store.create(owner, first);
    const auto *address = &record;
    store.create(owner, second);
    store.release(spare);
#if TDS_TELEMETRY_LEVEL >= 1
    require(store.live_count() == 2 && store.reserved_count() == 0,
            "accept and release did not update connection gauges");
#endif
    require(store.reserve() == spare && store.at(first.value) == address,
            "slot reuse moved a live record");

    record.rx.start();
    rejects(
        [&] {
            store.retire(first);
        },
        "store retired an original active operation");
    require(store.at(first.value) == address && record.rx.active(),
            "failed retirement changed the active record");
    record.rx.complete();
    rejects(
        [&] {
            store.retire(first);
        },
        "store retired before close completion");
    require(store.at(first.value) == address, "idle retirement released the live address");
#if TDS_TELEMETRY_LEVEL >= 1
    require(store.live_count() == 2 && store.reserved_count() == 1,
            "rejected retirement changed connection gauges");
#endif
    record.close_phase = ClosePhase::complete;
    store.retire(first);
    store.release(spare);
#if TDS_TELEMETRY_LEVEL >= 1
    require(store.live_count() == 1 && store.reserved_count() == 0,
            "terminal close or released reservation retained a diagnostic count");
#endif
}

void request_and_retirement_contracts()
{
    using tds::net::RequestPhase;
    Ring owner(1, true);
    auto &store = owner.connections;
    const auto slot = store.reserve();
    auto &connection = store.create(owner, slot);
    const auto original = connection.id();
    require(store.find(original) == &connection && !connection.retirable(),
            "new connection lookup or retirement state is invalid");
    require(!store.find({slot, original.generation + 1}) &&
                !store.find({tds::net::DirectSlot{1}, original.generation}),
            "lookup accepted a stale generation or out-of-range slot");
    rejects(
        [&] {
            connection.start_queued_write();
        },
        "write queued without claiming its request");
    rejects(
        [&] {
            connection.finish_response();
        },
        "response reset without response ownership");
    connection.claim_request();
    rejects(
        [&] {
            connection.claim_request();
        },
        "completed request was claimed twice");
    connection.start_queued_write();
    rejects(
        [&] {
            connection.claim_request();
        },
        "queued write was claimed twice");
    rejects(
        [&] {
            connection.start_queued_write();
        },
        "same write was enqueued twice");
    connection.close_phase = ClosePhase::complete;
    rejects(
        [&] {
            store.retire(slot);
        },
        "retirement discarded a queued database write");
    connection.start_response();
    rejects(
        [&] {
            connection.claim_request();
        },
        "response preparation released the request claim");
    rejects(
        [&] {
            connection.start_response();
        },
        "response storage was prepared twice");
    connection.rx.start();
    connection.tx.start();
    connection.rx.complete(true);
    connection.tx.complete();
    require(connection.request_phase == RequestPhase::responding && !connection.retirable(),
            "TX completion released the request or a live RX operation");
    connection.tx.start();
    require(connection.rx.request_cancel() && connection.tx.request_cancel(),
            "independent original operations did not accept cancellation");
    connection.rx.complete();
    connection.tx.acknowledge_cancel();
    require(!connection.retirable(), "retirement ignored opposite cancel completion orders");
    rejects(
        [&] {
            store.retire(slot);
        },
        "record retired before both cancellation pairs ended");
    connection.rx.acknowledge_cancel();
    require(!connection.retirable(), "record retired before the original send terminal");
    connection.tx.complete();
    connection.finish_response();
    require(connection.request_phase == RequestPhase::reading && connection.retirable(),
            "resolved response did not release its claim or retirement pins");
    connection.db_refs = 1;
    rejects(
        [&] {
            store.retire(slot);
        },
        "retirement ignored a database batch reference");
    connection.db_refs = 0;
    connection.work_queued = true;
    rejects(
        [&] {
            store.retire(slot);
        },
        "retirement ignored a queued work ticket");
    connection.work_queued = false;
    connection.work_active = true;
    rejects(
        [&] {
            store.retire(slot);
        },
        "retirement ignored an active work visit");
    connection.work_active = false;
    store.retire(slot);
    require(!store.find(original), "retired connection remained addressable by its handle");
    const auto reused = store.reserve();
    require(reused == slot, "retired slot was not reusable");
    auto &replacement = store.create(owner, reused);
    require(replacement.id().generation == original.generation + 1 && !store.find(original) &&
                store.find(replacement.id()) == &replacement,
            "slot reuse failed to advance its generation or rejected the new handle");
    replacement.claim_request();
    replacement.start_queued_write();
    replacement.abandon_request();
    require(replacement.request_phase == RequestPhase::reading,
            "resolved database failure retained the request claim");
    replacement.close_phase = ClosePhase::complete;
    store.retire(reused);
}

void held_completion_defers_retirement()
{
    FileLimit limited(128);
    Reactor reactor("127.0.0.1", 0);
    reactor.start();
    Client peer(reactor.port());
    const auto deadline = Clock::now() + 5s;
    await_completion(reactor, deadline);
    reactor.begin_turn();
    drain_turn(reactor, [](const Completion &) {
    });
    auto &connection = only_connection(reactor);
    const auto key = connection.id();
    auto &store = connection.ring.connections;
    Completion held;
    peer.send_byte();
    while (!held.connection)
    {
        await_completion(reactor, deadline);
        reactor.begin_turn();
        for (auto records = reactor.collect(); !records.empty(); records = reactor.collect())
        {
            for (auto &completion : records)
            {
                if (completion.kind == CompletionKind::received)
                {
                    held = std::move(completion);
                }
                completion.reset();
            }
        }
        reactor.finish_turn();
    }
    require(held.connection == &connection && connection.completion_refs == 1 &&
                held.buffer.bytes().front() == 'x',
            "moving a completion lost its connection pin or selected storage");
    reactor.close(connection);
    while (true)
    {
        const auto *current = store.find(key);
        require(current, "close retired a connection still owned by a held completion");
        if (current->close_phase == ClosePhase::complete)
        {
            break;
        }
        await_completion(reactor, deadline);
        reactor.begin_turn();
        drain_turn(reactor, [](Completion &completion) {
            completion.buffer.release();
        });
    }
    require(held.connection == store.find(key) && held.buffer.bytes().front() == 'x',
            "close invalidated an earlier collected completion");
    rejects(
        [&] {
            store.retire(key.slot);
        },
        "store ignored a held completion reference");
    held.reset();
    require(!store.find(key), "releasing the final completion pin did not retire the connection");
    reactor.request_stop();
    while (reactor.pending_count())
    {
        await_completion(reactor, deadline);
        reactor.begin_turn();
        drain_turn(reactor, [](Completion &completion) {
            completion.buffer.release();
        });
    }
}

void full_ready_preserves_cq_and_stop_progress()
{
    using tds::net::ConnectionKey;
    using tds::net::ReadyQueue;
    using tds::net::WorkReason;
    FileLimit limited(512);
    Reactor reactor("127.0.0.1", 0);
    reactor.start();
    Client peer(reactor.port());
    const auto deadline = Clock::now() + 5s;
    await_completion(reactor, deadline);
    reactor.begin_turn();
    drain_turn(reactor, [](const Completion &) {
    });
    auto &connection = only_connection(reactor);
    auto &owner = connection.ring;
    require(reactor.schedule(connection, WorkReason::input) &&
                reactor.schedule(connection, WorkReason::transmit) && owner.ready.size() == 1 &&
                connection.work_reasons == (WorkReason::input | WorkReason::transmit),
            "duplicate readiness created another ticket or lost its reason");
    std::vector<ConnectionKey> synthetic;
    for (unsigned index = 1; index < ReadyQueue::capacity; ++index)
    {
        auto &record = owner.connections.create(owner, owner.connections.reserve());
        synthetic.push_back(record.id());
        require(reactor.schedule(record, WorkReason::close),
                "ready fixture rejected available space");
    }
    auto &overflow = owner.connections.create(owner, owner.connections.reserve());
    const auto overflow_key = overflow.id();
    require(!reactor.schedule(overflow, WorkReason::close) && !overflow.work_queued &&
                overflow.work_reasons == WorkReason::close &&
                owner.ready.size() == ReadyQueue::capacity,
            "ready saturation silently lost a reason, ticket or its bound");
    reactor.request_stop();
    await_completion(reactor, deadline);
    const auto head = io_uring_smp_load_acquire(owner.queue.cq.khead);
    reactor.begin_turn();
    require(reactor.collect().empty() && io_uring_smp_load_acquire(owner.queue.cq.khead) == head &&
                !reactor.stopping(),
            "full work queue consumed a CQE without space for its possible work ticket");
    reactor.finish_turn();

    unsigned visits = 0;
    while (const auto key = owner.ready.pop())
    {
        auto *record = owner.connections.find(*key);
        require(record && record->work_queued, "ready queue lost its connection membership");
        record->work_queued = false;
        record->work_active = true;
        const auto remaining = owner.ready.size();
        require(reactor.schedule(*record, WorkReason::resume) && owner.ready.size() == remaining,
                "active visit submitted a duplicate work ticket");
        record->work_active = false;
        record->work_reasons = WorkReason::none;
        ++visits;
    }
    require(visits == ReadyQueue::capacity,
            "full work queue required unbounded visits to make room");
    // Synthetic records exercise registry work ownership without registering sockets
    synthetic.push_back(overflow_key);
    for (const auto key : synthetic)
    {
        auto *record = owner.connections.find(key);
        require(record, "synthetic ready record retired before its work was released");
        record->close_phase = ClosePhase::complete;
        require(owner.try_retire(key), "released synthetic work still pinned retirement");
    }
    reactor.begin_turn();
    unsigned stop_records = 0;
    for (auto records = reactor.collect(); !records.empty(); records = reactor.collect())
    {
        for (auto &record : records)
        {
            stop_records += record.operation.event() == Event::stop;
            record.reset();
        }
    }
    reactor.finish_turn();
    require(reactor.stopping() && stop_records == 1,
            "stop did not progress after one bounded full-queue visit budget");
    while (reactor.pending_count())
    {
        await_completion(reactor, deadline);
        reactor.begin_turn();
        drain_turn(reactor, [](Completion &completion) {
            completion.buffer.release();
        });
    }
}

void satellite_wake_read_lifetime()
{
    FileLimit limited(32);
    Reactor reactor("127.0.0.1", 0);
    reactor.start();
    require(reactor.rings().size() == 2, "wake-read fixture did not create a satellite");
    auto &primary = *reactor.rings().front();
    auto &satellite = *reactor.rings()[1];
    const auto quiet_pending = reactor.pending_count();
    for (const auto &ring : reactor.rings())
    {
        ring->submit();
    }
    require(io_uring_get_events(&primary.queue) >= 0 && io_uring_cq_ready(&primary.queue) == 0 &&
                satellite.wake_pending && satellite.wake_counter == 0 &&
                reactor.pending_count() == quiet_pending,
            "empty satellite read completed instead of remaining pending");

    const auto deadline = Clock::now() + 2s;
    for (const unsigned burst : {1U, 64U, 3U, 1U})
    {
        // The burst of 64 completes before submission of the newly armed primary read
        const bool already_signaled = burst == 64;
        if (!already_signaled)
        {
            primary.submit();
            require(io_uring_get_events(&primary.queue) >= 0 &&
                        io_uring_cq_ready(&primary.queue) == 0 && satellite.wake_pending &&
                        satellite.wake_counter == 0,
                    "rearmed empty satellite read did not remain pending");
        }
        for (unsigned index = 0; index < burst; ++index)
        {
            io_uring_prep_nop(satellite.entry({}));
        }
        satellite.submit();
        require(io_uring_cq_ready(&satellite.queue) == burst,
                "satellite NOP burst did not produce the expected CQEs");
        if (already_signaled)
        {
            pollfd ready{satellite.notification->get(), POLLIN, 0};
            require(::poll(&ready, 1, 0) == 1 && (ready.revents & POLLIN),
                    "satellite burst did not signal before the read was submitted");
            primary.submit();
        }
        while (io_uring_cq_ready(&primary.queue) == 0)
        {
            require(Clock::now() < deadline, "satellite wake read did not complete");
            const int result = io_uring_get_events(&primary.queue);
            require(result >= 0 || result == -EINTR, "wake-read get events failed");
        }
        io_uring_cqe *wake = nullptr;
        require(io_uring_peek_cqe(&primary.queue, &wake) == 0 &&
                    OperationId::from_raw(io_uring_cqe_get_data64(wake)).event() == Event::wake &&
                    OperationId::from_raw(io_uring_cqe_get_data64(wake)).index() == 1 &&
                    wake->res == static_cast<int>(sizeof(satellite.wake_counter)) &&
                    wake->flags == 0 && satellite.wake_counter > 0 &&
                    satellite.wake_counter <= burst,
                "satellite wake read lost its eight-byte result, counter or owner");
        std::uint64_t residual = 0;
        require(::read(satellite.notification->get(), &residual, sizeof(residual)) == -1 &&
                    errno == EAGAIN,
                "completed satellite read did not reset the eventfd counter");

        reactor.begin_turn();
        unsigned completions = 0;
        drain_turn(reactor, [&](const Completion &completion) {
            require(completion.kind == CompletionKind::control,
                    "satellite NOP burst exposed application I/O");
            ++completions;
        });
        require(completions == burst + 1 && reactor.pending_count() == quiet_pending &&
                    satellite.wake_pending && satellite.wake_counter == 0 &&
                    io_uring_cq_ready(&primary.queue) == 0 &&
                    io_uring_cq_ready(&satellite.queue) == 0,
                "wake drain lost a completion, ownership or its next empty read");
    }

    primary.submit();
    require(io_uring_get_events(&primary.queue) >= 0 && io_uring_cq_ready(&primary.queue) == 0 &&
                satellite.wake_pending,
            "shutdown fixture did not retain a pending empty wake read");
    reactor.request_stop();
    while (reactor.pending_count())
    {
        await_completion(reactor, deadline);
        reactor.begin_turn();
        drain_turn(reactor, [](const Completion &completion) {
            require(completion.kind == CompletionKind::control,
                    "wake-read shutdown exposed application I/O");
        });
    }
    require(reactor.stopping() && !satellite.wake_pending && primary.pending == 0 &&
                satellite.pending == 0 && io_uring_cq_ready(&primary.queue) == 0 &&
                io_uring_cq_ready(&satellite.queue) == 0,
            "shutdown retained the original wake read, a cancel CQE or satellite ownership");

    struct Unwind
    {
    };

    try
    {
        // Unwinding must cancel the primary read before releasing satellite-owned storage
        Reactor interrupted("127.0.0.1", 0);
        interrupted.start();
        require(interrupted.rings().size() == 2, "unwind fixture did not create a satellite");
        for (const auto &ring : interrupted.rings())
        {
            ring->submit();
        }
        auto &owner = *interrupted.rings().front();
        const auto &target = *interrupted.rings()[1];
        require(io_uring_get_events(&owner.queue) >= 0 && io_uring_cq_ready(&owner.queue) == 0 &&
                    target.wake_pending && target.wake_counter == 0,
                "unwind fixture did not retain a quiet pending read");
        throw Unwind{};
    }
    catch (const Unwind &)
    {
    }
}

void primary_progress_with_ready_satellite()
{
    constexpr rlim_t slots = 64;
    FileLimit limited(slots);

    // Send storage outlives the reactor, including failure cleanup and terminal cancellation
    std::string payload(256 * 1024, 'y');
    iovec vector{payload.data(), payload.size()};
    msghdr message{};
    message.msg_iov = &vector;
    message.msg_iovlen = 1;
    Reactor reactor("127.0.0.1", 0);
    bool listener_limited = false;
    for (int descriptor = 0; descriptor < static_cast<int>(slots); ++descriptor)
    {
        sockaddr_in address{};
        socklen_t length = sizeof(address);
        int listening = 0;
        socklen_t option_length = sizeof(listening);
        if (::getsockname(descriptor, reinterpret_cast<sockaddr *>(&address), &length) == 0 &&
            address.sin_family == AF_INET && address.sin_port == htons(reactor.port()) &&
            ::getsockopt(descriptor, SOL_SOCKET, SO_ACCEPTCONN, &listening, &option_length) == 0 &&
            listening)
        {
            const int size = 4096;
            require(::setsockopt(descriptor, SOL_SOCKET, SO_SNDBUF, &size, sizeof(size)) == 0,
                    "limit primary send buffer");
            listener_limited = true;
            break;
        }
    }
    require(listener_limited, "primary progress listener not found");
    reactor.start();
    Client peer(reactor.port(), 4096);
    const auto setup_deadline = Clock::now() + 5s;
    await_completion(reactor, setup_deadline);
    reactor.begin_turn();
    drain_turn(reactor, [](const Completion &completion) {
        require(completion.kind == CompletionKind::control, "unexpected progress setup payload");
    });
    auto &connection = only_connection(reactor);
    auto &primary = *reactor.rings().front();
    require(&connection.ring == &primary && reactor.rings().size() == 2,
            "reserved direct slots did not create a real satellite ring");
    require(primary.queue.flags & IORING_SETUP_TASKRUN_FLAG,
            "primary deferred work does not advertise TASKRUN");
    auto &satellite = *reactor.rings()[1];
    primary.submit();

    const auto add_satellite_completions = [&](unsigned count) {
        for (unsigned index = 0; index < count; ++index)
        {
            io_uring_prep_nop(satellite.entry({}));
        }
        satellite.submit();
    };
    add_satellite_completions(512);
    const auto busy_turn = [&](const auto &handler) {
        require(io_uring_cq_ready(&satellite.queue) >= 512,
                "satellite CQ was not ready before the primary wait shortcut");
        // No get_events, peek_cqe or readiness helper may process primary task work here
        reactor.begin_turn();
        drain_turn(reactor, handler);
        require(io_uring_cq_ready(&satellite.queue) >= 256,
                "satellite CQ became empty during the bounded production drain");
        add_satellite_completions(256);
    };
    const auto shutdown = [&] {
        reactor.request_stop();
        const auto deadline = Clock::now() + 5s;
        while (reactor.pending_count())
        {
            // Cleanup may force events only after the critical production-path checks end
            await_completion(reactor, deadline);
            reactor.begin_turn();
            drain_turn(reactor, [](Completion &completion) {
                completion.buffer.release();
            });
        }
    };
    try
    {
        // Settle the satellite notification before exercising a clean empty primary
        int settled;
        do
        {
            settled = io_uring_submit_and_get_events(&primary.queue);
        } while (settled == -EINTR);
        require(settled >= 0, "settle the primary satellite notification");
        reactor.begin_turn();
        drain_turn(reactor, [](const Completion &completion) {
            require(completion.kind == CompletionKind::control,
                    "unexpected application I/O before the clean primary turn");
        });
        primary.submit();
        require(io_uring_sq_ready(&primary.queue) == 0 && io_uring_cq_ready(&primary.queue) == 0 &&
                    !(io_uring_smp_load_acquire(primary.queue.sq.kflags) &
                      (IORING_SQ_TASKRUN | IORING_SQ_CQ_OVERFLOW)) &&
                    io_uring_cq_ready(&satellite.queue) == 256,
                "clean primary fixture retained work or lost its ready satellite");
        reactor.begin_turn();
        unsigned quiet_count = 0;
        drain_turn(reactor, [&](const Completion &completion) {
            require(completion.kind == CompletionKind::control,
                    "clean primary turn exposed application I/O");
            ++quiet_count;
        });
        require(quiet_count == 256 && io_uring_cq_ready(&primary.queue) == 0,
                "clean empty primary did not service its ready satellite");
        add_satellite_completions(512);

        peer.send_byte();
        bool received = false;
        const auto receive_deadline = Clock::now() + 2s;
        while (!received)
        {
            require(Clock::now() < receive_deadline,
                    "ready satellite starved the primary receive completion");
            busy_turn([&](Completion &completion) {
                if (completion.kind == CompletionKind::received)
                {
                    require(!received && completion.connection == &connection &&
                                completion.result == 1 && completion.buffer.bytes().size() == 1 &&
                                completion.buffer.bytes().front() == 'x',
                            "primary receive lost its target or payload");
                    completion.buffer.release();
                    received = true;
                }
                else
                {
                    require(completion.kind == CompletionKind::control,
                            "unexpected send before the primary send was submitted");
                }
            });
        }

        reactor.send(connection, message);
        std::size_t sent = 0;
        std::size_t delivered = 0;
        bool partial_send = false;
        const auto read_after = Clock::now() + 20ms;
        const auto send_deadline = Clock::now() + 2s;
        while (sent != payload.size() || delivered != payload.size())
        {
            require(Clock::now() < send_deadline,
                    "ready satellite starved the backpressured primary send");
            busy_turn([&](Completion &completion) {
                if (completion.kind == CompletionKind::sent)
                {
                    require(completion.connection == &connection && completion.result > 0 &&
                                static_cast<std::size_t>(completion.result) <=
                                    payload.size() - sent,
                            "primary send completion lost its target or byte count");
                    sent += static_cast<std::size_t>(completion.result);
                    if (sent != payload.size())
                    {
                        partial_send = true;
                        vector.iov_base = payload.data() + sent;
                        vector.iov_len = payload.size() - sent;
                        reactor.send(connection, message);
                    }
                }
                else
                {
                    require(completion.kind == CompletionKind::control,
                            "unexpected receive while checking primary send progress");
                }
            });
            if (Clock::now() >= read_after)
            {
                delivered += peer.drain_bytes('y');
                require(delivered <= payload.size(), "primary send duplicated payload bytes");
            }
        }
        require(partial_send, "primary send did not exercise real socket backpressure");

        const auto primary_slot = connection.slot;
        require(connection.rx.active(), "persistent RX ended during partial-send progress");
        reactor.request_stop();
        const auto stop_deadline = Clock::now() + 2s;
        while (!reactor.stopping() || primary.connections.at(primary_slot.value))
        {
            require(Clock::now() < stop_deadline,
                    "ready satellite starved primary stop or connection retirement");
            busy_turn([](const Completion &completion) {
                require(completion.kind == CompletionKind::control,
                        "unexpected application I/O while checking primary stop progress");
            });
        }
        require(peer.closed(stop_deadline), "primary retirement did not deliver TCP EOF");
        require(io_uring_cq_ready(&satellite.queue) >= 512,
                "satellite CQ was not continuously ready through primary TCP EOF");
    }
    catch (...)
    {
        const auto failure = std::current_exception();
        shutdown();
        std::rethrow_exception(failure);
    }
    shutdown();
}

void prime_primary_hot_turn(Reactor &reactor)
{
    auto &primary = *reactor.rings().front();
    require(primary.queue.features & IORING_FEAT_EXT_ARG,
            "primary timed-wait fixture requires extended arguments");
    for (unsigned index = 0; index < 2; ++index)
    {
        io_uring_prep_nop(primary.entry({}));
    }
    primary.submit();
    const auto deadline = Clock::now() + 2s;
    while (io_uring_cq_ready(&primary.queue) < 2)
    {
        require(Clock::now() < deadline, "primary hot-turn priming timed out");
        const int result = io_uring_get_events(&primary.queue);
        require(result >= 0 || result == -EINTR, "primary hot-turn get events failed");
    }
    reactor.begin_turn();
    unsigned count = 0;
    drain_turn(reactor, [&](const Completion &completion) {
        require(completion.kind == CompletionKind::control, "unexpected hot-turn payload");
        ++count;
    });
    require(count == 2, "primary hot turn did not drain exactly two completions");
    for (const auto &ring : reactor.rings())
    {
        require(io_uring_cq_ready(&ring->queue) == 0,
                "hot-turn fixture retained a ready primary or satellite CQ");
    }
}

void primary_progress_after_hot_turn()
{
    FileLimit limited(64);
    std::string payload(1, 'y');
    iovec vector{payload.data(), payload.size()};
    msghdr message{};
    message.msg_iov = &vector;
    message.msg_iovlen = 1;
    Reactor reactor("127.0.0.1", 0);
    reactor.start();
    Client peer(reactor.port());
    const auto deadline = Clock::now() + 2s;
    await_completion(reactor, deadline);
    reactor.begin_turn();
    drain_turn(reactor, [](const Completion &completion) {
        require(completion.kind == CompletionKind::control, "unexpected timed-wait setup payload");
    });
    auto &connection = only_connection(reactor);
    require(&connection.ring == reactor.rings().front().get() && reactor.rings().size() == 2,
            "timed-wait fixture did not retain its primary connection and quiet satellite");

    const auto sparse_turn = [&](const auto &handler) {
        for (const auto &ring : reactor.rings())
        {
            require(io_uring_cq_ready(&ring->queue) == 0,
                    "a ready CQ bypassed the sparse primary timed wait");
        }
        // No readiness helper may run deferred primary task work before the production wait
        reactor.begin_turn();
        unsigned count = 0;
        drain_turn(reactor, [&](Completion &completion) {
            ++count;
            handler(completion);
        });
        require(Clock::now() < deadline, "sparse primary timed wait lost progress");
        require(count > 0 && count < 32,
                "sparse primary timed wait did not drain fewer than 32 completions");
    };

    prime_primary_hot_turn(reactor);
    peer.send_byte();
    bool received = false;
    sparse_turn([&](Completion &completion) {
        require(completion.kind == CompletionKind::received && !received &&
                    completion.connection == &connection && completion.result == 1 &&
                    completion.buffer.bytes().size() == 1 &&
                    completion.buffer.bytes().front() == 'x',
                "timed primary receive lost its original operation or payload");
        completion.buffer.release();
        received = true;
    });
    require(received, "timed primary receive did not complete");

    prime_primary_hot_turn(reactor);
    reactor.send(connection, message);
    bool sent = false;
    sparse_turn([&](const Completion &completion) {
        require(completion.kind == CompletionKind::sent && !sent &&
                    completion.connection == &connection && completion.result == 1,
                "timed primary send lost its original operation or byte count");
        sent = true;
    });
    require(sent, "timed primary send did not complete");
    std::size_t delivered = 0;
    while (delivered != payload.size())
    {
        require(Clock::now() < deadline, "timed primary send lost its payload");
        delivered += peer.drain_bytes('y');
        require(delivered <= payload.size(), "timed primary send duplicated its payload");
        if (!delivered)
        {
            std::this_thread::sleep_for(1ms);
        }
    }

    prime_primary_hot_turn(reactor);
    require(connection.rx.active(), "persistent RX ended during sparse send progress");
    reactor.request_stop();
    sparse_turn([](const Completion &completion) {
        require(completion.kind == CompletionKind::control,
                "unexpected application payload during the timed primary stop");
    });
    require(reactor.stopping(), "sparse primary timed wait did not service stop");
    while (reactor.pending_count())
    {
        require(Clock::now() < deadline, "timed primary stop retained an original operation");
        reactor.begin_turn();
        drain_turn(reactor, [](const Completion &completion) {
            require(completion.kind == CompletionKind::control,
                    "timed primary stop exposed application I/O");
        });
    }
    require(peer.closed(deadline), "timed primary stop did not deliver TCP EOF");
}

void empty_primary_turn_returns_to_blocking_wait()
{
    FileLimit limited(64);
    Reactor reactor("127.0.0.1", 0);
    reactor.start();
    Client peer(reactor.port());
    const auto deadline = Clock::now() + 2s;
    await_completion(reactor, deadline);
    reactor.begin_turn();
    drain_turn(reactor, [](const Completion &completion) {
        require(completion.kind == CompletionKind::control, "unexpected idle-wait setup payload");
    });
    auto &connection = only_connection(reactor);
    require(&connection.ring == reactor.rings().front().get() && reactor.rings().size() == 2,
            "idle-wait fixture did not retain its primary connection and quiet satellite");

    const auto empty_timed_turn = [&] {
        prime_primary_hot_turn(reactor);
        // One empty timed turn must reset the next production wait to blocking wait-one
        reactor.begin_turn();
        unsigned count = 0;
        drain_turn(reactor, [&](const Completion &) {
            ++count;
        });
        require(count == 0 && Clock::now() < deadline,
                "idle primary timed turn did not finish empty");
    };

    empty_timed_turn();
    std::exception_ptr publish_failure;
    std::jthread receive_publisher([&] {
        std::this_thread::sleep_for(20ms);
        try
        {
            peer.send_byte();
        }
        catch (...)
        {
            publish_failure = std::current_exception();
            reactor.request_stop();
        }
    });
    // Returning empty before the worker publishes would expose repeated timed polling
    reactor.begin_turn();
    unsigned received = 0;
    drain_turn(reactor, [&](Completion &completion) {
        require(completion.kind == CompletionKind::received && received == 0 &&
                    completion.connection == &connection && completion.result == 1 &&
                    completion.buffer.bytes().size() == 1 &&
                    completion.buffer.bytes().front() == 'x',
                "idle primary receive lost its original operation or payload");
        completion.buffer.release();
        ++received;
    });
    receive_publisher.join();
    if (publish_failure)
    {
        std::rethrow_exception(publish_failure);
    }
    require(received == 1 && Clock::now() < deadline,
            "idle primary wait returned empty before the worker receive");

    empty_timed_turn();
    std::jthread stop_publisher([&] {
        std::this_thread::sleep_for(20ms);
        reactor.request_stop();
    });
    reactor.begin_turn();
    drain_turn(reactor, [](const Completion &completion) {
        require(completion.kind == CompletionKind::control,
                "unexpected application payload during the idle primary stop");
    });
    stop_publisher.join();
    require(reactor.stopping() && Clock::now() < deadline,
            "idle primary wait returned empty before the worker stop");
    while (reactor.pending_count())
    {
        require(Clock::now() < deadline, "idle primary stop retained an original operation");
        reactor.begin_turn();
        drain_turn(reactor, [](const Completion &completion) {
            require(completion.kind == CompletionKind::control,
                    "idle primary stop exposed application I/O");
        });
    }
    require(peer.closed(deadline), "idle primary stop did not deliver TCP EOF");
}

void fixed_cq_order_and_growth()
{
    FileLimit limited(32);
    Reactor reactor("127.0.0.1", 0);
    reactor.start();
    require(reactor.rings().size() == 2, "accept reservations did not create two rings");
    const auto deadline = Clock::now() + 5s;
    const auto add_nops = [](Ring &ring, unsigned count) {
        for (unsigned index = 0; index < count; ++index)
        {
            io_uring_prep_nop(ring.entry({}));
        }
        ring.submit();
    };
    for (const auto &ring : reactor.rings())
    {
        add_nops(*ring, 8);
    }
    await_completion(reactor, deadline);
    reactor.begin_turn();
    drain_turn(reactor, [](const Completion &completion) {
        require(completion.kind == CompletionKind::control, "unexpected setup payload");
    });

    // The first accept appends a ring that must join the same traversal
    Client peer(reactor.port());
    bool accept_ready = false;
    while (!accept_ready)
    {
        await_completion(reactor, deadline);
        for (const auto &ring : reactor.rings())
        {
            unsigned head;
            io_uring_cqe *cqe;
            io_uring_for_each_cqe(&ring->queue, head, cqe)
            {
                const auto operation = OperationId::from_raw(io_uring_cqe_get_data64(cqe));
                accept_ready =
                    accept_ready || (operation.event() == Event::accept && cqe->res >= 0);
            }
        }
        require(Clock::now() < deadline, "accept CQE was not ready before the growth turn");
    }
    constexpr std::array<std::size_t, 3> expected{0, 1, 2};
    for (unsigned turn = 0; turn < 3; ++turn)
    {
        std::array<unsigned, 3> heads{};
        for (std::size_t index = 0; index < reactor.rings().size(); ++index)
        {
            auto &ring = *reactor.rings()[index];
            heads[index] = *ring.queue.cq.khead;
            add_nops(ring, turn ? 256 : 512);
        }
        std::vector<std::size_t> order;
        const auto observe_advances = [&] {
            require(reactor.rings().size() <= heads.size(), "unexpected extra ring growth");
            for (std::size_t index = 0; index < reactor.rings().size(); ++index)
            {
                const auto head = *reactor.rings()[index]->queue.cq.khead;
                if (head != heads[index])
                {
                    require(head - heads[index] == 256, "CQ drain exceeded or missed its budget");
                    order.push_back(index);
                    heads[index] = head;
                }
            }
        };
        reactor.begin_turn();
        Completion completion;
        bool grew = false;
        while (reactor.next(completion))
        {
            require(completion.kind == CompletionKind::control, "unexpected growth payload");
            if (turn == 0 && !grew && reactor.rings().size() == 3)
            {
                add_nops(*reactor.rings()[2], 512);
                grew = true;
            }
            observe_advances();
        }
        observe_advances();
        reactor.finish_turn();
        require(reactor.rings().size() == 3, "accept did not grow a third ring during traversal");
        require(order.size() == 3, "CQ traversal missed or revisited a ring after growth");
        for (std::size_t index = 0; index < order.size(); ++index)
        {
            require(order[index] == expected[index], "CQ traversal did not preserve ring order");
        }
    }
    reactor.request_stop();
    while (reactor.pending_count())
    {
        await_completion(reactor, deadline);
        reactor.begin_turn();
        drain_turn(reactor, [](Completion &completion) {
            completion.buffer.release();
        });
    }
    require(peer.closed(deadline), "shutdown after ring growth did not close the peer");
}

void cq_wrap_and_last_consumer_publication()
{
    FileLimit limited(64);
    Reactor reactor("127.0.0.1", 0);
    auto &primary = *reactor.rings().front();
    const auto publish = [&](unsigned count) {
        for (unsigned index = 0; index < count; ++index)
        {
            io_uring_prep_nop(primary.entry({}));
        }
        int result;
        do
        {
            result = io_uring_submit_and_wait(&primary.queue, count);
        } while (result == -EINTR);
        require(result >= 0 && io_uring_cq_ready(&primary.queue) == count,
                "CQ wrap fixture did not publish its real NOP batch");
    };
    const auto consume = [&](unsigned count) {
        const auto head = io_uring_smp_load_acquire(primary.queue.cq.khead);
        reactor.begin_turn();
        Completion completion;
        for (unsigned index = 0; index < count; ++index)
        {
            require(reactor.next(completion) && completion.kind == CompletionKind::control,
                    "CQ cursor lost a NOP before its bounded snapshot ended");
            require(io_uring_smp_load_acquire(primary.queue.cq.khead) == head,
                    "CQ head advanced before the last completion consumer finished");
        }
        require(!reactor.next(completion), "CQ cursor exceeded its bounded snapshot");
        reactor.finish_turn();
        require(io_uring_smp_load_acquire(primary.queue.cq.khead) == head + count,
                "CQ finish did not publish the complete consumed batch");
    };

    // Reach the physical wrap with real completions instead of rewriting shared cursors
    unsigned prefix = primary.queue.cq.ring_entries - 128;
    while (prefix)
    {
        const auto count = std::min(prefix, 256U);
        publish(count);
        consume(count);
        prefix -= count;
    }
    publish(512);
    consume(256);
    require(io_uring_cq_ready(&primary.queue) == 256,
            "CQ wrap consumed the next turn's batch or lost its cap");
    consume(256);
    require(primary.pending == 0 && io_uring_cq_ready(&primary.queue) == 0,
            "CQ wrap retained or duplicated a completion");
}

void exhausted_receive_retries()
{
    Reactor reactor("127.0.0.1", 0);
    reactor.start();
    auto &owner = *reactor.rings().front();
    constexpr unsigned clients_count = ReceiveBuffers::count + 512;
    const auto deadline = Clock::now() + 5s;
    std::vector<std::unique_ptr<Client>> clients;
    for (unsigned index = 0; index < clients_count; ++index)
    {
        clients.push_back(std::make_unique<Client>(reactor.port()));
    }
    unsigned accepted = 0;
    while (accepted != clients_count)
    {
        await_completion(reactor, deadline);
        reactor.begin_turn();
        drain_turn(reactor, [](const Completion &completion) {
            require(completion.kind == CompletionKind::control, "unexpected accept payload");
        });
        accepted = 0;
        for (std::size_t slot = 0; slot < owner.connections.size(); ++slot)
        {
            accepted += owner.connections.at(slot) != nullptr;
        }
    }
    owner.submit();
#if TDS_TELEMETRY_LEVEL >= 1
    require(owner.connections.live_count() == clients_count &&
                owner.connections.reserved_count() == 64 &&
                owner.statistics.totals.receive_submissions == clients_count &&
                owner.statistics.totals.receive_completions == 0,
            "idle receive submissions or connection gauges were not counted correctly");
#endif
    for (const auto &client : clients)
    {
        client->send_byte();
    }
    std::vector<ReceiveBufferLease> held;
    held.reserve(ReceiveBuffers::count);
    std::vector<bool> received(owner.connections.size());
    unsigned total_received = 0;
    unsigned terminal_receives = 0;
    const auto retain = [&](Completion &completion) {
        if (completion.operation.event() == Event::receive &&
            !(completion.flags & IORING_CQE_F_MORE))
        {
            require(completion.result == -ENOBUFS,
                    "pool exhaustion produced an unexpected receive terminal");
            ++terminal_receives;
        }
        if (completion.kind != CompletionKind::received)
        {
            require(completion.kind == CompletionKind::control, "unexpected exhaustion send");

            return;
        }
        const auto slot = completion.connection->slot.value;
        require(!received[slot] && completion.result == 1 &&
                    (completion.flags & IORING_CQE_F_MORE) &&
                    completion.buffer.bytes().front() == 'x',
                "exhaustion duplicated or corrupted a receive");
        received[slot] = true;
        ++total_received;
#if TDS_TELEMETRY_LEVEL == 2
        require(completion.connection->receive_enobufs_since_ns == 0,
                "successful receive retained its first ENOBUFS timestamp");
#endif
        held.push_back(std::move(completion.buffer));
    };
    // MORE data and original ENOBUFS terminals are distinct CQEs
    while (held.size() != ReceiveBuffers::count ||
           owner.deferred_receives.size() < clients_count - ReceiveBuffers::count ||
           io_uring_cq_ready(&owner.queue))
    {
        await_completion(reactor, deadline);
        reactor.begin_turn();
        drain_turn(reactor, retain);
    }
    require(held.size() == ReceiveBuffers::count, "the selected pool was not fully retained");
#if TDS_TELEMETRY_LEVEL == 2
    std::vector<std::pair<Connection *, std::uint64_t>> first_deferred;
    for (std::size_t slot = 0; slot < owner.connections.size(); ++slot)
    {
        auto *connection = owner.connections.at(slot);
        if (connection && connection->receive_enobufs_since_ns)
        {
            first_deferred.emplace_back(connection, connection->receive_enobufs_since_ns);
        }
    }
    require(first_deferred.size() >= clients_count - ReceiveBuffers::count &&
                owner.statistics.interval.enobufs_to_successful_receive_service_ns.count() == 0,
            "first ENOBUFS timestamps missed deferrals or recorded unfinished recovery");
#endif

    const auto control_turn = [&](const auto &handler) {
        io_uring_prep_nop(owner.entry({}));
        await_completion(reactor, deadline);
        reactor.begin_turn();
        drain_turn(reactor, handler);
    };
    constexpr unsigned control_turns = 16;
    unsigned control_cqes = 0;
    const auto started = Clock::now();
    for (unsigned turn = 0; turn < control_turns; ++turn)
    {
        control_turn([&](Completion &completion) {
            require(completion.kind == CompletionKind::control,
                    "a receive completed without an available buffer");
            retain(completion);
            ++control_cqes;
        });
    }
    const auto elapsed = Clock::now() - started;
    const auto probes = std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count() / 4;
    std::printf("Receive retry probe: %u control CQEs in %u turns, %.3f ms\n", control_cqes,
                control_turns, std::chrono::duration<double, std::milli>(elapsed).count());
    require(control_cqes <= control_turns + 3 + static_cast<unsigned>(probes),
            "exhausted receive retries kept generating CQEs without buffer progress");

    // No refill yet: a bounded probe must still run, rather than parking the FIFO forever
    const auto before_probe = terminal_receives;
    std::this_thread::sleep_for(5ms);
    control_turn(retain);
    control_turn(retain);
    require(terminal_receives > before_probe,
            "timed retry never probed an exhausted receive without returned buffers");
#if TDS_TELEMETRY_LEVEL == 2
    for (const auto &[connection, first] : first_deferred)
    {
        require(connection->receive_enobufs_since_ns == first,
                "repeated ENOBUFS restarted the application deferral clock");
    }
#endif

    held.clear();
    // Return once, then retain the recovered buffers so a second retry wave gets no refill event
    while (total_received != clients_count)
    {
        control_turn(retain);
    }
    require(held.size() == clients_count - ReceiveBuffers::count,
            "one refill did not recover every client's queued input across bounded turns");
#if TDS_TELEMETRY_LEVEL >= 1
    unsigned active_receives = 0;
    for (std::size_t slot = 0; slot < owner.connections.size(); ++slot)
    {
        const auto *connection = owner.connections.at(slot);
        active_receives += connection && connection->rx.active();
    }
    require(owner.statistics.totals.receive_enobufs >= clients_count - ReceiveBuffers::count &&
                owner.statistics.totals.receive_completions == clients_count + terminal_receives &&
                owner.statistics.totals.receive_submissions == terminal_receives + active_receives,
            "receive counters confused MORE data, original terminals or control CQEs");
#endif
#if TDS_TELEMETRY_LEVEL == 2
    require(owner.statistics.interval.enobufs_to_successful_receive_service_ns.count() ==
                    clients_count - ReceiveBuffers::count &&
                owner.statistics.interval.enobufs_to_successful_receive_service_ns.max >= 5'000'000,
            "recovery timing missed an episode or excluded the buffer-deferred wait");
#endif
    held.clear();
    reactor.request_stop();
    while (reactor.pending_count())
    {
        await_completion(reactor, deadline);
        reactor.begin_turn();
        drain_turn(reactor, [](const Completion &completion) {
            require(completion.kind == CompletionKind::control,
                    "exhaustion shutdown reached application I/O");
        });
    }
}

void transient_receive_preserves_kind_and_cancel_intent()
{
    FileLimit limited(128);
    for (const int error : {-EAGAIN, -EINTR})
    {
        // Mode zero retries normally; the other modes force both cancellation CQE orders
        for (const unsigned cancel_order : {0U, 1U, 2U})
        {
            Reactor reactor("127.0.0.1", 0);
            reactor.start();
            Client peer(reactor.port());
            const auto deadline = Clock::now() + 5s;
            await_completion(reactor, deadline);
            reactor.begin_turn();
            drain_turn(reactor, [](const Completion &) {
            });
            auto &connection = only_connection(reactor);
            auto &owner = connection.ring;
            reactor.cancel_receive(connection);
            while (connection.rx.busy())
            {
                await_completion(reactor, deadline);
                reactor.begin_turn();
                drain_turn(reactor, [](const Completion &) {
                });
            }
            const auto quiet_pending = owner.pending;
            const auto original = OperationId::connection(Event::receive, connection.id());
            connection.rx.start();
            io_uring_prep_nop(owner.entry(original));
            owner.submit();
            const auto await_cqes = [&](unsigned count) {
                while (io_uring_cq_ready(&owner.queue) < count)
                {
                    require(Clock::now() < deadline, "transient receive injection timed out");
                    require(io_uring_get_events(&owner.queue) >= 0,
                            "collect transient receive injection CQEs");
                }
            };
            await_cqes(1);
            if (cancel_order)
            {
                // The NOP is terminal already, so cancellation acknowledges ENOENT
                reactor.cancel_receive(connection);
                owner.submit();
                await_cqes(2);
            }
            io_uring_cqe *receive = nullptr;
            io_uring_cqe *acknowledgement = nullptr;
            unsigned cursor;
            io_uring_cqe *cqe;
            io_uring_for_each_cqe(&owner.queue, cursor, cqe)
            {
                const auto event = OperationId::from_raw(cqe->user_data).event();
                if (event == Event::receive)
                {
                    receive = cqe;
                }
                if (event == Event::cancel_receive)
                {
                    acknowledgement = cqe;
                }
            }
            require(receive && receive->res == 0 && receive->flags == 0,
                    "transient receive fixture did not complete its tagged NOP");
            receive->res = error;
            if (cancel_order)
            {
                require(acknowledgement && acknowledgement->res == -ENOENT,
                        "terminal NOP cancellation did not acknowledge ENOENT");
                const bool acknowledgement_first = cancel_order == 2;
                io_uring_cqe *first = nullptr;
                require(io_uring_peek_cqe(&owner.queue, &first) == 0,
                        "transient receive CQEs disappeared before reordering");
                if ((first == acknowledgement) != acknowledgement_first)
                {
                    std::swap(*receive, *acknowledgement);
                }
            }
            reactor.begin_turn();
            unsigned completions = 0;
            drain_turn(reactor, [&](const Completion &) {
                ++completions;
            });
            if (cancel_order)
            {
                require(completions == 2 && !connection.rx.busy() &&
                            connection.rx.cancel_requested() && owner.pending == quiet_pending &&
                            io_uring_sq_ready(&owner.queue) == 0,
                        "transient cancellation lost intent or submitted a replacement receive");
            }
            else
            {
                require(completions == 1 && connection.rx.active() &&
                            !connection.rx.cancel_requested() && owner.pending == quiet_pending + 1,
                        "transient receive failed to rearm exactly one original");
                peer.send_byte();
                await_completion(reactor, deadline);
                reactor.begin_turn();
                bool received = false;
                drain_turn(reactor, [&](const Completion &completion) {
                    if (completion.kind == CompletionKind::received)
                    {
                        require(completion.result == 1 &&
                                    completion.buffer.bytes().front() == 'x' &&
                                    (completion.flags & IORING_CQE_F_MORE),
                                "transient retry did not submit a genuine multishot receive");
                        received = true;
                    }
                });
                require(received && connection.rx.active(), "multishot retry lost persistent RX");
            }
            reactor.request_stop();
            while (reactor.pending_count())
            {
                await_completion(reactor, deadline);
                reactor.begin_turn();
                drain_turn(reactor, [](Completion &completion) {
                    completion.buffer.release();
                });
            }
        }
    }
}

void successful_terminal_receive_resumes_after_consumption()
{
    FileLimit limited(128);
    Reactor reactor("127.0.0.1", 0);
    reactor.start();
    Client peer(reactor.port());
    const auto deadline = Clock::now() + 5s;
    await_completion(reactor, deadline);
    reactor.begin_turn();
    drain_turn(reactor, [](const Completion &) {
    });
    auto &connection = only_connection(reactor);
    auto &owner = connection.ring;
    reactor.cancel_receive(connection);
    while (connection.rx.busy())
    {
        await_completion(reactor, deadline);
        reactor.begin_turn();
        drain_turn(reactor, [](const Completion &) {
        });
    }
    const auto quiet_pending = owner.pending;
    owner.receive_buffers->take_retry_credits(ReceiveBuffers::count);
    connection.rx.start();
    auto *request = owner.entry(OperationId::connection(Event::receive, connection.id()));
    // A real one-shot SQE supplies a successful terminal without changing live multishot flags
    io_uring_prep_recv(request, static_cast<int>(connection.slot.value), nullptr,
                       ReceiveBuffers::size, 0);
    request->flags |= IOSQE_BUFFER_SELECT | IOSQE_FIXED_FILE;
    request->buf_group = ReceiveBuffers::group;
    peer.send_byte();
    await_completion(reactor, deadline);
    reactor.begin_turn();
    unsigned received = 0;
    for (auto records = reactor.collect(); !records.empty(); records = reactor.collect())
    {
        for (auto &completion : records)
        {
            if (completion.kind == CompletionKind::received)
            {
                require(completion.connection == &connection && completion.result == 1 &&
                            !(completion.flags & IORING_CQE_F_MORE),
                        "positive terminal fixture did not produce a genuine terminal receive");
                owner.backlog.push(connection.input, std::move(completion.buffer));
                ++received;
            }
            completion.reset();
        }
    }
    reactor.finish_turn();
    require(received == 1 && !connection.rx.busy() && connection.rx_paused &&
                connection.input.count == 1 && owner.pending == quiet_pending,
            "successful RX terminal was not accounted once and marked for resume");
    reactor.resume_receive(connection);
    require(!connection.rx.busy() && owner.pending == quiet_pending,
            "terminal receive rearmed before its queued data was consumed");
    auto lease = owner.backlog.pop(connection.input);
    require(lease && lease.bytes().size() == 1 && lease.bytes().front() == 'x',
            "successful terminal lost its selected input during handoff");
    lease.release();
    reactor.resume_receive(connection);
    reactor.resume_receive(connection);
    require(connection.rx.active() && !connection.rx_paused && owner.pending == quiet_pending + 1 &&
                io_uring_sq_ready(&owner.queue) == 1,
            "consuming a positive terminal failed to arm exactly one replacement receive");
    peer.send_byte();
    await_completion(reactor, deadline);
    reactor.begin_turn();
    received = 0;
    drain_turn(reactor, [&](const Completion &completion) {
        if (completion.kind == CompletionKind::received)
        {
            require(completion.result == 1 && (completion.flags & IORING_CQE_F_MORE) &&
                        completion.buffer.bytes().front() == 'x',
                    "positive-terminal recovery did not resume genuine persistent RX");
            ++received;
        }
    });
    require(received == 1 && connection.rx.active() && owner.pending == quiet_pending + 1,
            "replacement RX overlapped an original or retired on MORE");
    require(owner.receive_buffers->take_retry_credits(ReceiveBuffers::count) == 1,
            "positive-terminal handoff or resumed data leaked or duplicated a selected buffer");
    reactor.request_stop();
    while (reactor.pending_count())
    {
        await_completion(reactor, deadline);
        reactor.begin_turn();
        drain_turn(reactor, [](Completion &completion) {
            completion.buffer.release();
        });
    }
}

void multishot_operation_lifetimes()
{
    FileLimit limited(128);
    std::string payload(8192, 'y');
    iovec vector{payload.data(), payload.size()};
    msghdr message{};
    message.msg_iov = &vector;
    message.msg_iovlen = 1;
    Reactor reactor("127.0.0.1", 0);
    reactor.start();
    Client peer(reactor.port());
    const auto deadline = Clock::now() + 5s;
    await_completion(reactor, deadline);
    reactor.begin_turn();
    drain_turn(reactor, [](const Completion &completion) {
        require(completion.kind == CompletionKind::control, "unexpected multishot setup data");
    });
    auto &connection = only_connection(reactor);
    auto &owner = connection.ring;
    reactor.cancel_receive(connection);
    rejects(
        [&] {
            reactor.receive(connection);
        },
        "receive rearmed before its previous cancellation pair completed");
    while (connection.rx.busy())
    {
        await_completion(reactor, deadline);
        reactor.begin_turn();
        drain_turn(reactor, [](const Completion &completion) {
            require(completion.kind == CompletionKind::control || completion.result == -ECANCELED,
                    "quiet cancellation exposed payload");
        });
    }

    reactor.receive(connection);
    const auto persistent_pending = owner.pending;
    peer.send_byte();
    std::vector<ReceiveBufferLease> held;
    const auto keep_receive = [&](Completion &completion) {
        if (completion.kind == CompletionKind::received && completion.result > 0)
        {
            require(completion.connection == &connection && completion.result == 1 &&
                        completion.buffer.bytes().size() == 1 &&
                        completion.buffer.bytes().front() == 'x',
                    "multishot CQE lost its selected data");
            require(completion.flags & IORING_CQE_F_MORE,
                    "multishot data terminalized before cancellation or EOF");
            held.push_back(std::move(completion.buffer));
        }
    };
    while (held.empty())
    {
        await_completion(reactor, deadline);
        reactor.begin_turn();
        drain_turn(reactor, keep_receive);
    }
    require(connection.rx.active() && owner.pending == persistent_pending,
            "MORE decremented pending count or retired the original receive");
    reactor.send(connection, message);
    require(connection.rx.active() && connection.tx.active(),
            "send did not coexist with persistent RX");
    rejects(
        [&] {
            reactor.send(connection, message);
        },
        "second send reused storage still borrowed by its original");
    peer.send_byte();
    owner.submit();
    // Queue selected input before cancellation; the terminal/ack order is kernel-owned
    while (io_uring_cq_ready(&owner.queue) < 2 && Clock::now() < deadline)
    {
        require(io_uring_get_events(&owner.queue) >= 0, "queue multishot input and send result");
    }
    reactor.cancel_receive(connection);
    const auto after_cancel = owner.pending;
    reactor.cancel_receive(connection);
    require(owner.pending == after_cancel, "duplicate targeted cancellation submitted another SQE");
    rejects(
        [&] {
            reactor.receive(connection);
        },
        "cancelled RX rearmed before original and acknowledgement completed");
    std::size_t sent = 0;
    while (connection.rx.busy() || connection.tx.busy() || held.size() != 2)
    {
        await_completion(reactor, deadline);
        reactor.begin_turn();
        drain_turn(reactor, [&](Completion &completion) {
            keep_receive(completion);
            if (completion.kind == CompletionKind::sent)
            {
                require(completion.connection == &connection && completion.result > 0,
                        "concurrent send lost its connection or signed result");
                sent += static_cast<std::size_t>(completion.result);
            }
        });
    }
    require(sent == payload.size() && owner.pending + 1 == persistent_pending,
            "cancel pair or simultaneous TX changed terminal-only pending accounting");
    require(peer.drain_bytes('y') == sent, "concurrent send changed its immutable payload");
    require(held[0].bytes().front() == 'x' && held[1].bytes().front() == 'x',
            "cancellation invalidated held selected buffers");
    held.clear();
    const auto key = connection.id();
    auto &store = owner.connections;
    reactor.receive(connection);
    peer.finish_writing();
    bool eof_received = false;
    while (!eof_received)
    {
        await_completion(reactor, deadline);
        reactor.begin_turn();
        drain_turn(reactor, [&](Completion &completion) {
            require(completion.kind == CompletionKind::control || completion.result == 0,
                    "EOF fixture exposed unexpected payload");
            if (completion.operation.event() == Event::receive && completion.result == 0)
            {
                require(completion.operation.connection_key() == key &&
                            !(completion.flags & IORING_CQE_F_MORE),
                        "EOF lost its identity or retained multishot MORE");
                const auto *current = store.find(key);
                require(current && current->rx_eof && !current->rx.busy() &&
                            !current->rx.cancelling(),
                        "natural EOF retained an original or cancellation acknowledgement");
                reactor.close(*completion.connection);
                eof_received = true;
            }
        });
    }
    reactor.request_stop();
    while (reactor.pending_count())
    {
        await_completion(reactor, deadline);
        reactor.begin_turn();
        drain_turn(reactor, [](Completion &completion) {
            completion.buffer.release();
        });
    }
}

void close_live_independent_operations()
{
    FileLimit limited(128);
    std::string payload(256 * 1024, 'y');
    iovec vector{payload.data(), payload.size()};
    msghdr message{};
    message.msg_iov = &vector;
    message.msg_iovlen = 1;
    Reactor reactor("127.0.0.1", 0);
    reactor.start();
    Client peer(reactor.port(), 4096);
    const auto deadline = Clock::now() + 5s;
    await_completion(reactor, deadline);
    reactor.begin_turn();
    drain_turn(reactor, [](const Completion &) {
    });
    auto &connection = only_connection(reactor);
    const auto key = connection.id();
    auto &store = connection.ring.connections;
    reactor.send(connection, message);
    require(connection.rx.active() && connection.tx.active(),
            "live close fixture did not own independent originals");
    reactor.close(connection);
    require(store.find(key) == &connection && connection.closing() && connection.rx.cancelling() &&
                connection.tx.cancelling(),
            "close did not pin and cancel both original operations");
    rejects(
        [&] {
            store.retire(key.slot);
        },
        "close request retired live originals");
    rejects(
        [&] {
            reactor.close(connection);
        },
        "live close submitted duplicate cancellation pairs");
    while (store.find(key))
    {
        await_completion(reactor, deadline);
        reactor.begin_turn();
        drain_turn(reactor, [](Completion &completion) {
            completion.buffer.release();
        });
    }
    reactor.request_stop();
    while (reactor.pending_count())
    {
        await_completion(reactor, deadline);
        reactor.begin_turn();
        drain_turn(reactor, [](Completion &completion) {
            completion.buffer.release();
        });
    }
}

void multishot_natural_exhaustion()
{
    FileLimit limited(128);
    std::string payload(1, 'y');
    iovec vector{payload.data(), payload.size()};
    msghdr message{};
    message.msg_iov = &vector;
    message.msg_iovlen = 1;
    Reactor reactor("127.0.0.1", 0);
    reactor.start();
    Client peer(reactor.port());
    const auto deadline = Clock::now() + 5s;
    await_completion(reactor, deadline);
    reactor.begin_turn();
    drain_turn(reactor, [](const Completion &) {
    });
    auto &connection = only_connection(reactor);
    reactor.cancel_receive(connection);
    while (connection.rx.busy())
    {
        await_completion(reactor, deadline);
        reactor.begin_turn();
        drain_turn(reactor, [](const Completion &) {
        });
    }
    reactor.receive(connection);
    std::vector<ReceiveBufferLease> held;
    held.reserve(ReceiveBuffers::count);
    // Isolate returned-buffer progress from the fallback's timed permission
    connection.ring.next_receive_probe = deadline;
    bool exhausted = false;
    while (!exhausted)
    {
        peer.send_byte();
        await_completion(reactor, deadline);
        reactor.begin_turn();
        for (auto records = reactor.collect(); !records.empty(); records = reactor.collect())
        {
            for (auto &completion : records)
            {
                if (completion.kind == CompletionKind::received && completion.result > 0)
                {
                    require(completion.result == 1 && completion.buffer.bytes().front() == 'x' &&
                                (completion.flags & IORING_CQE_F_MORE),
                            "exhaustion lost multishot data or MORE");
                    held.push_back(std::move(completion.buffer));
                }
                if (completion.operation.event() == Event::receive && completion.result == -ENOBUFS)
                {
                    require(!(completion.flags & IORING_CQE_F_MORE) && !connection.rx.active() &&
                                !connection.rx.cancelling() && !connection.rx.cancel_requested(),
                            "natural ENOBUFS invented an outstanding original or cancellation");
                    exhausted = true;
                }
                completion.reset();
            }
        }
        reactor.finish_turn();
    }
    require(held.size() == ReceiveBuffers::count,
            "multishot exhausted before every provided buffer was retained");
    auto &owner = connection.ring;
    reactor.send(connection, message);
    bool sent = false;
    unsigned quiet_pending = 0;
    while (!sent)
    {
        await_completion(reactor, deadline);
        reactor.begin_turn();
        for (auto records = reactor.collect(); !records.empty(); records = reactor.collect())
        {
            for (auto &completion : records)
            {
                require(completion.kind != CompletionKind::received,
                        "TX at RX exhaustion invented input without a provided buffer");
                if (completion.kind == CompletionKind::sent)
                {
                    require(!sent && completion.connection == &connection && completion.result == 1,
                            "TX needed an RX lease or completed its byte count incorrectly");
                    sent = true;
                }
                completion.reset();
            }
        }
        if (sent)
        {
            require(connection.receive_deferred && !connection.rx.busy() &&
                        owner.staged_count == 0 && owner.ready.empty() &&
                        io_uring_sq_ready(&owner.queue) == 0 &&
                        io_uring_cq_ready(&owner.queue) == 0,
                    "returned-credit fixture retained another source of runnable work");
            quiet_pending = owner.pending;
            // Application consumption returns this credit after collection's retry pass
            held.back().release();
        }
        reactor.finish_turn();
    }
    require(peer.drain_bytes('y') == payload.size() && held.size() == ReceiveBuffers::count,
            "TX at RX exhaustion lost bytes or released held input");
    reactor.begin_turn();
    require(connection.rx.active() && !connection.receive_deferred &&
                owner.pending == quiet_pending + 1,
            "wait slept before preparing RX from a credit returned after collection");
    unsigned resumed = 0;
    drain_turn(reactor, [&](Completion &completion) {
        if (completion.kind == CompletionKind::received)
        {
            require(completion.connection == &connection && completion.result == 1 &&
                        (completion.flags & IORING_CQE_F_MORE) &&
                        completion.buffer.bytes().front() == 'x',
                    "returned-credit retry lost the already readable byte");
            completion.buffer.release();
            ++resumed;
        }
    });
    require(resumed == 1, "returned-credit retry did not service input before collecting again");
    reactor.request_stop();
    while (reactor.pending_count())
    {
        await_completion(reactor, deadline);
        reactor.begin_turn();
        drain_turn(reactor, [](Completion &completion) {
            completion.buffer.release();
        });
    }
    held.clear();
}

void fatal_unwind_with_live_operations_and_lease()
{
    struct Unwind
    {
    };

    FileLimit limited(128);
    std::string payload(256 * 1024, 'y');
    iovec vector{payload.data(), payload.size()};
    msghdr message{};
    message.msg_iov = &vector;
    message.msg_iovlen = 1;
    try
    {
        Reactor reactor("127.0.0.1", 0);
        reactor.start();
        Client peer(reactor.port(), 4096);
        const auto deadline = Clock::now() + 5s;
        await_completion(reactor, deadline);
        reactor.begin_turn();
        drain_turn(reactor, [](const Completion &) {
        });
        auto &connection = only_connection(reactor);
        peer.send_byte();
        std::vector<ReceiveBufferLease> held;
        while (held.empty())
        {
            await_completion(reactor, deadline);
            reactor.begin_turn();
            drain_turn(reactor, [&](Completion &completion) {
                if (completion.kind == CompletionKind::received)
                {
                    held.push_back(std::move(completion.buffer));
                }
            });
        }
        require(connection.rx.active(), "fatal teardown lost persistent RX before send");
        reactor.send(connection, message);
        connection.ring.submit();
        require(connection.rx.active() && connection.tx.active() &&
                    held.front().bytes().front() == 'x',
                "fatal teardown fixture lost live originals or selected storage");
        // Held leases unwind before their reactor; external send storage outlives its kernel
        throw Unwind{};
    }
    catch (const Unwind &)
    {
    }
}

void stale_selected_completion_returns_lease()
{
    FileLimit limited(128);
    Reactor reactor("127.0.0.1", 0);
    reactor.start();
    Client peer(reactor.port());
    const auto deadline = Clock::now() + 5s;
    await_completion(reactor, deadline);
    reactor.begin_turn();
    drain_turn(reactor, [](const Completion &) {
    });
    auto &connection = only_connection(reactor);
    auto &owner = connection.ring;
    auto &buffers = *owner.receive_buffers;
    require(buffers.take_retry_credits(ReceiveBuffers::count) == 0,
            "stale completion fixture already returned a buffer");
    peer.send_byte();
    io_uring_cqe *selected = nullptr;
    while (!selected)
    {
        await_completion(reactor, deadline);
        unsigned cursor;
        io_uring_cqe *cqe;
        io_uring_for_each_cqe(&owner.queue, cursor, cqe)
        {
            const auto operation = OperationId::from_raw(io_uring_cqe_get_data64(cqe));
            if (operation.event() == Event::receive)
            {
                selected = cqe;
                break;
            }
        }
    }
    require(selected->res == 1 && (selected->flags & IORING_CQE_F_BUFFER) &&
                (selected->flags & IORING_CQE_F_MORE),
            "stale completion fixture did not select persistent RX input");
    const auto current = connection.id();
    // Inject an unmatched generation into a real selected-buffer CQE
    selected->user_data =
        OperationId::connection(Event::receive, {current.slot, current.generation + 1}).raw();
    reactor.begin_turn();
    drain_turn(reactor, [](const Completion &completion) {
        require(completion.kind == CompletionKind::control && !completion.connection &&
                    completion.buffer.bytes().empty(),
                "stale completion reached a connection or exported selected storage");
    });
    require(connection.rx.active() && !connection.rx_eof &&
                owner.connections.find(current) == &connection,
            "stale operation mutated the current connection");
    require(buffers.take_retry_credits(ReceiveBuffers::count) == 1 &&
                buffers.take_retry_credits(ReceiveBuffers::count) == 0,
            "stale selected buffer was leaked or returned more than once");
    reactor.close(connection);
    reactor.request_stop();
    while (reactor.pending_count())
    {
        await_completion(reactor, deadline);
        reactor.begin_turn();
        drain_turn(reactor, [](Completion &completion) {
            completion.buffer.release();
        });
    }
}

void close_and_late_accept()
{
    Reactor reactor("127.0.0.1", 0);
    reactor.start();
    const auto deadline = Clock::now() + 5s;
    Client initial(reactor.port());
    await_completion(reactor, deadline);
    reactor.begin_turn();
    drain_turn(reactor, [](const Completion &completion) {
        require(completion.kind == CompletionKind::control, "unexpected initial application I/O");
    });

    auto &connection = only_connection(reactor);
    auto &store = connection.ring.connections;
    const auto slot = connection.slot;
    const auto *address = &connection;
    require(connection.rx.active(), "accept did not arm the original receive");
    rejects(
        [&] {
            store.retire(slot);
        },
        "active receive allowed slot retirement");
    require(store.at(slot.value) == address && connection.rx.active(),
            "rejected retirement changed original-operation ownership");

    Client idle(reactor.port());
    await_completion(reactor, deadline);
    reactor.begin_turn();
    drain_turn(reactor, [](const Completion &completion) {
        require(completion.kind == CompletionKind::control, "idle accept reached application I/O");
    });
    Connection *idle_connection = nullptr;
    for (std::size_t index = 0; index < store.size(); ++index)
    {
        if (auto *candidate = store.at(index); candidate && candidate != address)
        {
            require(!idle_connection, "more than one idle connection");
            idle_connection = candidate;
        }
    }
    require(idle_connection && idle_connection->rx.active(),
            "idle peer did not retain its original receive in the same ring");

    initial.send_byte();
    bool received = false;
    while (!received)
    {
        await_completion(reactor, deadline);
        reactor.begin_turn();
        drain_turn(reactor, [&](Completion &completion) {
            if (completion.kind == CompletionKind::received)
            {
                require(completion.connection == address && completion.result == 1 &&
                            completion.buffer.bytes().size() == 1 &&
                            completion.buffer.bytes().front() == 'x',
                        "original receive did not preserve its connection and buffer");
                completion.buffer.release();
                rejects(
                    [&] {
                        store.retire(slot);
                    },
                    "receive CQE retired before close CQE");
                reactor.close(*completion.connection);
                require(store.at(slot.value) == address && completion.connection->closing(),
                        "submitting close retired its record before the final close CQE");
                rejects(
                    [&] {
                        reactor.close(*completion.connection);
                    },
                    "duplicate close submitted another terminal operation");
                received = true;
            }
        });
    }
    while (store.at(slot.value))
    {
        await_completion(reactor, deadline);
        reactor.begin_turn();
        drain_turn(reactor, [&](const Completion &completion) {
            require(completion.kind == CompletionKind::control, "unexpected I/O while closing");
            if (auto *closing = store.at(slot.value))
            {
                require(closing == address && closing->closing(),
                        "intermediate close completion changed record ownership");
                rejects(
                    [&] {
                        reactor.close(*closing);
                    },
                    "intermediate close completion allowed a duplicate close");
            }
        });
    }

    // Cursed fixed-file table references can outlive close_direct's CQE and delay TCP EOF
    require(idle_connection->rx.active(), "idle receive completed before the EOF check");
    require(initial.closed(Clock::now() + 1s),
            "retired connection did not deliver TCP EOF while an unrelated receive was pending");
    const auto reused = store.reserve();
    require(reused == slot && !store.at(reused.value), "close did not return its direct slot");
    store.release(reused);

    for (unsigned attempt = 0; attempt < 2; ++attempt)
    {
        idle.send_byte();
        bool idle_received = false;
        while (!idle_received)
        {
            await_completion(reactor, deadline);
            reactor.begin_turn();
            drain_turn(reactor, [&](Completion &completion) {
                if (completion.kind == CompletionKind::received)
                {
                    require(completion.connection == idle_connection && completion.result == 1 &&
                                completion.buffer.bytes().size() == 1 &&
                                completion.buffer.bytes().front() == 'x',
                            "closing another connection damaged the idle peer's receive");
                    completion.buffer.release();
                    require(idle_connection->rx.active(),
                            "another close ended the idle peer's persistent RX");
                    idle_received = true;
                }
                else
                {
                    require(completion.kind == CompletionKind::control,
                            "unexpected send while checking the idle peer");
                }
            });
        }
    }

    reactor.request_stop();
    await_completion(reactor, deadline);
    reactor.begin_turn();
    auto &queue = reactor.rings().front()->queue;
    io_uring_cqe *head = nullptr;
    require(io_uring_peek_cqe(&queue, &head) == 0 && head != nullptr,
            "stop did not produce a queued CQE");
    const auto stop = OperationId::from_raw(io_uring_cqe_get_data64(head));
    require(stop.event() == Event::stop && head->res >= 0,
            "stop CQE was not first before late accept");

    Client late(reactor.port());
    bool late_accepted = false;
    while (!late_accepted && Clock::now() < deadline)
    {
        const int result = io_uring_get_events(&queue);
        require(result >= 0 || result == -EINTR, "late accept get events failed");
        unsigned cursor = 0;
        io_uring_cqe *cqe = nullptr;
        io_uring_for_each_cqe(&queue, cursor, cqe)
        {
            const auto operation = OperationId::from_raw(io_uring_cqe_get_data64(cqe));
            late_accepted |= operation.event() == Event::accept && cqe->res >= 0;
        }
        if (!late_accepted)
        {
            std::this_thread::sleep_for(1ms);
        }
    }
    require(late_accepted, "successful late accept was not queued behind stop");
    const auto no_application_io = [](const Completion &completion) {
        require(completion.kind == CompletionKind::control,
                "late shutdown accept reached application I/O");
    };
    drain_turn(reactor, no_application_io);
    require(reactor.stopping(), "queued stop did not enter shutdown");
    while (reactor.pending_count())
    {
        await_completion(reactor, deadline);
        reactor.begin_turn();
        drain_turn(reactor, no_application_io);
    }

    require(late.closed(deadline), "late accepted client was left open");
    require(idle.closed(deadline), "idle peer remained open after shutdown");
    for (const auto &ring : reactor.rings())
    {
        auto &connections = ring->connections;
#if TDS_TELEMETRY_LEVEL >= 1
        require(connections.live_count() == 0 && connections.reserved_count() == 0,
                "shutdown retained live records or accept reservations in its gauges");
#endif
        std::vector<bool> returned(connections.size());
        std::size_t count = 0;
        while (connections.has_free_slot())
        {
            const auto free = connections.reserve();
            require(free.value < returned.size() && !returned[free.value] &&
                        !connections.at(free.value),
                    "shutdown duplicated a slot or retained a live connection");
            returned[free.value] = true;
            ++count;
        }
        require(count == connections.size(), "shutdown lost an accept or connection slot");
        for (unsigned index = 0; index < connections.size(); ++index)
        {
            connections.release(tds::net::DirectSlot{index});
        }
    }
}

class TelemetryArtifact
{
  public:
    TelemetryArtifact()
    {
        if (const auto *previous = std::getenv("TDS_TELEMETRY_PATH"))
        {
            previous_ = previous;
        }
        const int descriptor = ::mkstemp(path_.data());
        require(descriptor >= 0, "create transport telemetry artifact");
        ::close(descriptor);
        if (::setenv("TDS_TELEMETRY_PATH", path_.c_str(), 1) != 0)
        {
            ::unlink(path_.c_str());
            throw std::runtime_error("set transport telemetry destination");
        }
    }

    ~TelemetryArtifact()
    {
        if (previous_)
        {
            ::setenv("TDS_TELEMETRY_PATH", previous_->c_str(), 1);
        }
        else
        {
            ::unsetenv("TDS_TELEMETRY_PATH");
        }
        ::unlink(path_.c_str());
    }

    const char *path() const noexcept
    {
        return path_.c_str();
    }

    bool empty() const
    {
        struct stat metadata{};
        require(::stat(path_.c_str(), &metadata) == 0, "read transport artifact size");

        return metadata.st_size == 0;
    }

  private:
    std::string path_ = "/tmp/tds-io-telemetry-XXXXXX";
    std::optional<std::string> previous_;
};

#if TDS_TELEMETRY_LEVEL >= 1
std::uint64_t number(yyjson_val *object, const char *key)
{
    const auto *value = yyjson_obj_get(object, key);
    require(yyjson_is_uint(value), "transport artifact lost an unsigned integer");

    return yyjson_get_uint(value);
}
#endif

void telemetry_hook_contracts()
{
    using tds::telemetry::IoTelemetry;
    TelemetryArtifact artifact;
    FileLimit limited(128);
    Reactor reactor("127.0.0.1", 0);
    auto &owner = *reactor.rings().front();
    const auto slot = owner.connections.reserve();
    owner.connections.reserve();
    auto &connection = owner.connections.create(owner, slot);
    IoTelemetry collector;
    {
        const auto run = collector.run(reactor);
#if TDS_TELEMETRY_LEVEL < 2
        // Basic and off scopes must not inspect even an absent Exchange
        const auto inactive_handler = collector.handler(connection);
        collector.receive(connection, ReceiveBufferLease{}).attach(connection);
        collector.send_prepared(connection);
        collector.send_completed(Completion{});
#endif
        ReceiveBufferLease selected(owner.receive_buffers.get(), 0, 1);
        IoTelemetry::receive_completed(connection, 1, selected);
#if TDS_TELEMETRY_LEVEL == 2
        const auto receive_ns = owner.statistics.receive_service_ns[selected.id()];
        const auto before_retention = tds::timing::now_ns();
        require(receive_ns > 0 && receive_ns <= before_retention,
                "receive service time was not sampled before buffer retention");
#endif
        owner.backlog.push(connection.input, std::move(selected));
        ReceiveBufferLease following(owner.receive_buffers.get(), 1, 1);
        IoTelemetry::receive_completed(connection, 1, following);
#if TDS_TELEMETRY_LEVEL == 2
        const auto following_ns = owner.statistics.receive_service_ns[following.id()];
        require(owner.statistics.receive_service_ns[0] == receive_ns &&
                    following_ns >= before_retention,
                "a later receive changed an earlier held buffer's service time");
#endif
        owner.backlog.push(connection.input, std::move(following));
        auto buffered = owner.backlog.pop(connection.input);
        auto next_buffered = owner.backlog.pop(connection.input);
        const auto received = collector.receive(connection, buffered);
        const auto next_received = collector.receive(connection, next_buffered);
        connection.exchange = owner.acquire_exchange();
        next_received.attach(connection);
#if TDS_TELEMETRY_LEVEL == 2
        require(connection.exchange->receive_serviced_ns == following_ns,
                "backlog consumption lost the later buffer's exact service time");
#endif
        received.attach(connection);
#if TDS_TELEMETRY_LEVEL == 2
        require(connection.exchange->receive_serviced_ns == receive_ns,
                "backlog consumption replaced the original buffer's service time");
#endif
        const auto accept = [&](tds::AcceptedHead head) {
            auto &session = connection.exchange->session;
            session.reset();
            session.append("GET /health HTTP/1.1\r\nHost: local\r\n\r\n");
            require(session.consume().valid && session.headers_pending(),
                    "parse hook fixture headers");
            session.accept_headers(std::move(head));
            require(session.consume().valid && session.complete(), "complete hook fixture request");
        };
        accept(tds::HealthHead{});
        {
            auto sample = collector.handler(connection);
            sample.finish();
            sample.finish();
        }
        try
        {
            const auto sample = collector.handler(connection);
            throw std::runtime_error("handler fixture failure");
        }
        catch (const std::runtime_error &)
        {
        }
        accept(tds::CreateHead{});
        {
            const auto sample = collector.handler(connection);
        }
        accept(tds::LikeHead{{}, "1"});
        {
            const auto sample = collector.handler(connection);
        }
        try
        {
            const auto sample = collector.write_batch(3);
            throw std::runtime_error("write fixture failure");
        }
        catch (const std::runtime_error &)
        {
        }

        auto pooled = owner.acquire_exchange();
        owner.recycle_exchange(std::move(pooled));
        pooled = owner.acquire_exchange();
        owner.recycle_exchange(std::move(pooled));
        Completion completion;
        IoTelemetry::receive_prepared(connection);
        IoTelemetry::receive_completed(connection, -ENOBUFS, ReceiveBufferLease{});
        IoTelemetry::receive_deferred(connection);
#if TDS_TELEMETRY_LEVEL == 2
        const auto first_enobufs = connection.receive_enobufs_since_ns;
        IoTelemetry::receive_deferred(connection);
        require(first_enobufs > 0 && connection.receive_enobufs_since_ns == first_enobufs,
                "repeated deferral replaced the first ENOBUFS time");
#endif
        IoTelemetry::receive_completed(connection, 1, buffered);
#if TDS_TELEMETRY_LEVEL == 2
        require(connection.receive_enobufs_since_ns == 0 &&
                    owner.statistics.interval.enobufs_to_successful_receive_service_ns.count() == 1,
                "successful receive did not resolve its deferral sample");
#endif
        for (const int result : {-EAGAIN, -ECANCELED})
        {
            collector.send_prepared(connection);
            IoTelemetry::send_serviced(completion);
            completion.connection = &connection;
            completion.result = result;
            connection.close_phase = ClosePhase::requested;
            collector.send_completed(completion);
        }
#if TDS_TELEMETRY_LEVEL == 2
        require(owner.statistics.interval.send_attempt_prepare_to_cqe_service_ns.count() == 2 &&
                    connection.exchange->receive_serviced_ns == receive_ns,
                "failed or closing sends lost samples or changed buffered receive time");
#endif
        IoTelemetry::drain_started(owner);
        owner.deferred_receives.push_back(connection.id());
        IoTelemetry::drain_finished(owner, 3);
        owner.deferred_receives.pop_front();
#if TDS_TELEMETRY_LEVEL >= 1
        const auto overflow = io_uring_smp_load_acquire(owner.queue.cq.koverflow);
        owner.statistics.previous_overflow = overflow - 3U;
#endif
        collector.report(reactor, false);
        require(artifact.empty(), "collection wrote a transport artifact before finalization");
#if TDS_TELEMETRY_LEVEL == 2
        require(collector.history().size() == 0, "early report captured a profile window");
#endif
        collector.report(reactor, true);
#if TDS_TELEMETRY_LEVEL == 2
        const auto &window = collector.history().at(0);
        const auto &ring = window.rings[0];
        require(window.start_ns > 0 && window.end_ns >= window.start_ns &&
                    window.timings.handler_wall_ns.count() == 2 &&
                    window.timings.receive_cqe_service_to_handler_start_ns.count() == 2 &&
                    window.timings.write_batch_size.count() == 1 &&
                    window.timings.write_batch_size.total == 3 &&
                    window.timings.write_batch_wall_ns.count() == 1,
                "profile scope classification, completion or unwind accounting changed");
        require(ring.counters.cqes == 3 && ring.counters.receive_submissions == 1 &&
                    ring.counters.receive_completions == 4 && ring.counters.receive_enobufs == 1 &&
                    ring.counters.cq_overflow_counter_delta == 3 &&
                    ring.counters.empty_cq_samples == 1 &&
                    ring.timings.cq_ready_before_drain.count() == 1 &&
                    ring.timings.cq_ready_before_drain.total == 0 &&
                    ring.timings.send_attempt_prepare_to_cqe_service_ns.count() == 2 &&
                    ring.timings.deferred_receive_queue_depth.total == 1,
                "profile window lost ring counters or timing samples");
        require(owner.statistics.totals.cqes == 3 &&
                    owner.statistics.interval.send_attempt_prepare_to_cqe_service_ns.count() == 0 &&
                    owner.statistics.previous_overflow == overflow,
                "profile capture lost cumulative counters or its overflow baseline");
#endif
        IoTelemetry::drain_started(owner);
        IoTelemetry::drain_finished(owner, 2);
        collector.report(reactor, true);
#if TDS_TELEMETRY_LEVEL == 2
        const auto &next = collector.history().at(1);
        require(next.start_ns == window.end_ns && next.rings[0].counters.cqes == 2 &&
                    next.rings[0].counters.receive_completions == 0 &&
                    next.rings[0].counters.cq_overflow_counter_delta == 0 &&
                    next.rings[0].counters.empty_cq_samples == 1 &&
                    next.rings[0].timings.cq_ready_before_drain.count() == 1 &&
                    next.rings[0].timings.cq_ready_before_drain.total == 0 &&
                    next.timings.handler_wall_ns.count() == 0,
                "consecutive profile windows reused interval values");
        for (std::size_t index = 0;
             index < tds::telemetry::History<tds::telemetry::IoSnapshot>::capacity; ++index)
        {
            collector.report(reactor, true);
        }
        require(collector.history().size() == 128 && collector.history().overwritten() == 2 &&
                    collector.totals().rings[0].counters.cqes == 5 &&
                    collector.totals().timings.handler_wall_ns.count() == 2 &&
                    collector.totals()
                            .rings[0]
                            .timings.send_attempt_prepare_to_cqe_service_ns.count() == 2,
                "bounded history overwrite lost cumulative transport totals");
        for (std::size_t index = 1; index < collector.history().size(); ++index)
        {
            require(collector.history().at(index - 1).end_ns ==
                        collector.history().at(index).start_ns,
                    "profile history is not chronological");
        }
#endif
        require(artifact.empty(), "profile capture formatted or exported transport telemetry");
    }
#if TDS_TELEMETRY_LEVEL == 0
    require(artifact.empty(), "off telemetry exported a transport record");
#else
    const auto release = [](yyjson_doc *document) noexcept {
        yyjson_doc_free(document);
    };
    yyjson_read_err error{};
    const std::unique_ptr<yyjson_doc, decltype(release)> document(
        yyjson_read_file(artifact.path(), 0, nullptr, &error), release);
    require(document != nullptr, "parse the single final transport JSON record");
    auto *root = yyjson_doc_get_root(document.get());
    const auto *mode = yyjson_obj_get(root, "mode");
    const auto *source = yyjson_obj_get(root, "source");
    require(yyjson_is_str(mode) && yyjson_is_str(source) &&
                std::string_view(yyjson_get_str(mode)) ==
                    tds::telemetry::name(tds::telemetry::level) &&
                std::string_view(yyjson_get_str(source)) == "transport",
            "transport artifact mode or source changed");
    auto *totals = yyjson_obj_get(root, "totals");
    auto *rings = yyjson_obj_get(totals, "rings");
    require(yyjson_is_arr(rings) && yyjson_arr_size(rings) == 1,
            "transport artifact lost its ring snapshot");
    auto *ring = yyjson_arr_get(rings, 0);
    require(number(ring, "cqes") == 5 && number(ring, "receive_submissions") == 1 &&
                number(ring, "receive_completions") == 4 && number(ring, "receive_enobufs") == 1 &&
                number(ring, "cq_overflow_counter_delta") == 3 &&
                number(ring, "exchange_allocations") == 2 &&
                number(ring, "exchange_pool_hits") == 1 && number(ring, "pooled_exchanges") == 1 &&
                number(ring, "live_connection_records") == 1 &&
                number(ring, "reserved_accept_slots") == 1,
            "transport artifact lost cumulative counters or final gauges");
    auto *windows = yyjson_obj_get(root, "windows");
    require(yyjson_is_arr(windows), "transport artifact lost its windows array");
#if TDS_TELEMETRY_LEVEL == 1
    require(yyjson_arr_size(windows) == 0 && number(root, "overwritten_windows") == 0 &&
                yyjson_is_null(yyjson_obj_get(totals, "start_ns")) &&
                yyjson_obj_get(totals, "metrics") == nullptr &&
                yyjson_obj_get(ring, "metrics") == nullptr,
            "basic telemetry retained profile timing data");
#else
    require(yyjson_arr_size(windows) == 128 && number(root, "overwritten_windows") == 3 &&
                number(totals, "start_ns") > 0,
            "profile final snapshot or explicit overwrite accounting changed");
#endif
#endif
}

#if TDS_TELEMETRY_LEVEL == 2
void telemetry_history_growth_after_wrap()
{
    using namespace tds::telemetry;
    TelemetryArtifact artifact;
    FileLimit limited(32);
    Reactor reactor("127.0.0.1", 0);
    IoTelemetry collector;
    const auto run = collector.run(reactor);
    constexpr auto capacity = History<IoSnapshot>::capacity;
    auto &primary = *reactor.rings().front();
    IoTelemetry::drain_finished(primary, 3);
    primary.statistics.interval.cq_ready_before_drain.record(9);
    {
        const auto batch = collector.write_batch(7);
    }
    for (std::size_t index = 0; index < capacity + 2; ++index)
    {
        collector.report(reactor, true);
    }
    require(collector.history().size() == capacity && collector.history().overwritten() == 2 &&
                collector.history().at(0).ring_count == 1 &&
                collector.history().at(capacity - 1).rings[0].counters.cqes == 0 &&
                collector.history().at(capacity - 1).timings.write_batch_size.count() == 0 &&
                collector.totals().rings[0].counters.cqes == 3 &&
                collector.totals().rings[0].timings.cq_ready_before_drain.total == 9 &&
                collector.totals().timings.write_batch_size.total == 7,
            "single-ring history wrap retained interval data or lost cumulative samples");

    reactor.start();
    require(reactor.rings().size() == 2, "accept reservations did not grow the profile reactor");
    collector.report(reactor, true);
    const auto &second = collector.history().at(capacity - 1);
    require(second.ring_count == 2 && second.rings[1].ring == 1 &&
                second.rings[1].reserved_accept_slots == 32 &&
                second.rings[1].live_connection_records == 0 &&
                second.rings[1].timings.cq_ready_before_drain.count() == 0 &&
                collector.totals().ring_count == 2,
            "a reused history slot missed the first dynamically added ring");

    Client peer(reactor.port());
    const auto deadline = Clock::now() + 5s;
    while (reactor.rings().size() < 3)
    {
        await_completion(reactor, deadline);
        reactor.begin_turn();
        drain_turn(reactor, [](const Completion &completion) {
            require(completion.kind == CompletionKind::control,
                    "unexpected application payload during profile ring growth");
        });
        require(Clock::now() < deadline, "profile ring growth timed out");
    }
    require(reactor.rings().size() == 3, "accept grew an unexpected number of profile rings");
    auto &third = *reactor.rings()[2];
    third.statistics.interval = {};
    third.statistics.interval.cq_ready_before_drain.record(17);
    collector.report(reactor, true);
    const auto &latest = collector.history().at(capacity - 1);
    require(collector.history().at(0).ring_count == 1 && latest.ring_count == 3 &&
                latest.rings[2].ring == 2 && latest.rings[2].reserved_accept_slots == 1 &&
                latest.rings[2].live_connection_records == 0 &&
                latest.rings[2].timings.cq_ready_before_drain.count() == 1 &&
                latest.rings[2].timings.cq_ready_before_drain.total == 17 &&
                collector.totals().ring_count == 3 &&
                collector.totals().rings[2].timings.cq_ready_before_drain.total == 17,
            "wrapped history growth missed a new ring or changed earlier window sizes");
    for (std::size_t index = 0; index < capacity; ++index)
    {
        collector.report(reactor, true);
    }
    for (std::size_t index = 0; index < capacity; ++index)
    {
        const auto &window = collector.history().at(index);
        require(window.ring_count == 3 && window.rings[2].ring == 2 &&
                    window.rings[2].timings.cq_ready_before_drain.count() == 0 &&
                    window.timings.write_batch_size.count() == 0 &&
                    (index == 0 || window.start_ns == collector.history().at(index - 1).end_ns),
                "ring growth left stale interval data or broke chronological slot reuse");
    }
    require(collector.history().overwritten() == capacity + 4 &&
                collector.totals().timings.write_batch_size.total == 7 &&
                collector.totals().rings[2].timings.cq_ready_before_drain.total == 17 &&
                artifact.empty(),
            "post-growth history wrap changed cumulative totals, loss counts or export timing");
    reactor.request_stop();
    while (reactor.pending_count())
    {
        await_completion(reactor, deadline);
        reactor.begin_turn();
        drain_turn(reactor, [](Completion &completion) {
            completion.buffer.release();
        });
    }
    require(peer.closed(deadline), "profile ring-growth shutdown did not close the peer");
}
#endif
} // namespace

int main()
{
    // A broken blocking path must fail the test process rather than hang the suite
    ::alarm(15);
    try
    {
        reservation_and_record_guards();
        request_and_retirement_contracts();
        held_completion_defers_retirement();
        full_ready_preserves_cq_and_stop_progress();
        satellite_wake_read_lifetime();
        primary_progress_with_ready_satellite();
        primary_progress_after_hot_turn();
        empty_primary_turn_returns_to_blocking_wait();
        fixed_cq_order_and_growth();
        cq_wrap_and_last_consumer_publication();
        exhausted_receive_retries();
        transient_receive_preserves_kind_and_cancel_intent();
        successful_terminal_receive_resumes_after_consumption();
        multishot_operation_lifetimes();
        close_live_independent_operations();
        multishot_natural_exhaustion();
        fatal_unwind_with_live_operations_and_lease();
        stale_selected_completion_returns_lease();
        close_and_late_accept();
        telemetry_hook_contracts();
#if TDS_TELEMETRY_LEVEL == 2
        telemetry_history_growth_after_wrap();
#endif
        ::alarm(0);
        std::puts("I/O reservation, satellite wake-read ownership, fixed CQ order/growth, "
                  "primary progress with a ready satellite, "
                  "CQ wrap and last-consumer publication, "
                  "sparse timed and idle primary waits, "
                  "original-CQE lifetime, "
                  "prompt TCP EOF and late-accept shutdown checks passed");

        return 0;
    }
    catch (const std::exception &error)
    {
        ::alarm(0);
        std::fprintf(stderr, "I/O lifetime checks failed: %s\n", error.what());

        return 1;
    }
}
