#include "net/ring.hpp"
#include "net/work.hpp"

#include <array>
#include <cerrno>
#include <cstdio>
#include <exception>
#include <stdexcept>
#include <sys/socket.h>
#include <unistd.h>
#include <utility>

namespace
{
using namespace tds::net;

void require(bool condition, const char *message)
{
    if (!condition)
    {
        throw std::runtime_error(message);
    }
}

void ready_queue_bounds_and_wrap()
{
    ReadyQueue ready;
    require(ready.empty() && !ready.pop() && ready.remaining() == ReadyQueue::capacity,
            "empty ready queue exposed work");
    for (unsigned cycle = 1; cycle <= 3; ++cycle)
    {
        for (unsigned index = 0; index < ReadyQueue::capacity; ++index)
        {
            require(ready.push({DirectSlot{index}, cycle}), "ready queue rejected available space");
        }
        require(!ready.push({DirectSlot{500}, cycle}) && !ready.remaining() &&
                    ready.size() == ReadyQueue::capacity,
                "full ready queue dropped work or grew beyond its bound");
        for (unsigned index = 0; index < ReadyQueue::capacity / 2; ++index)
        {
            require(ready.pop() == ConnectionKey{DirectSlot{index}, cycle},
                    "ready queue reordered its original prefix");
        }
        for (unsigned index = 0; index < ReadyQueue::capacity / 2; ++index)
        {
            require(ready.push({DirectSlot{index}, cycle + 100}),
                    "wrapped ready queue rejected space");
        }
        for (unsigned index = ReadyQueue::capacity / 2; index < ReadyQueue::capacity; ++index)
        {
            require(ready.pop() == ConnectionKey{DirectSlot{index}, cycle},
                    "wrapped insertion overtook carried work");
        }
        for (unsigned index = 0; index < ReadyQueue::capacity / 2; ++index)
        {
            require(ready.pop() == ConnectionKey{DirectSlot{index}, cycle + 100},
                    "wrapped ready queue lost a new generation");
        }
        require(ready.empty() && !ready.pop(), "ready queue retained a consumed ticket");
    }
    ready.push({DirectSlot{9}, 1});
    ready.clear();
    require(ready.empty() && ready.remaining() == ReadyQueue::capacity && !ready.pop(),
            "ready clear retained work or capacity");
}

class SelectedInput
{
  public:
    explicit SelectedInput(Ring &owner) : owner_(owner)
    {
        require(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, sockets_.data()) == 0,
                "provided-buffer fixture socketpair");
    }

    ~SelectedInput()
    {
        ::close(sockets_[0]);
        ::close(sockets_[1]);
    }

    ReceiveBufferLease receive(char byte, bool exhausted = false)
    {
        require(::send(sockets_[0], &byte, 1, MSG_NOSIGNAL) == 1, "provided-buffer fixture send");
        return take(exhausted);
    }

    ReceiveBufferLease receive_queued()
    {
        return take(false);
    }

  private:
    ReceiveBufferLease take(bool exhausted)
    {
        auto *request = owner_.entry();
        io_uring_prep_recv(request, sockets_[1], nullptr, ReceiveBuffers::size, 0);
        request->flags |= IOSQE_BUFFER_SELECT;
        request->buf_group = ReceiveBuffers::group;
        owner_.submit();
        io_uring_cqe *cqe = nullptr;
        int result;
        do
        {
            result = io_uring_wait_cqe(&owner_.queue, &cqe);
        } while (result == -EINTR);
        require(result == 0 && cqe, "provided-buffer fixture completion");
        const auto count = cqe->res;
        const auto flags = cqe->flags;
        io_uring_cqe_seen(&owner_.queue, cqe);
        --owner_.pending;
        if (exhausted)
        {
            require(count == -ENOBUFS && !(flags & IORING_CQE_F_BUFFER),
                    "fully retained backlog still exposed a provided buffer");
            return {};
        }
        const auto id = flags >> IORING_CQE_BUFFER_SHIFT;
        require(count == 1 && (flags & IORING_CQE_F_BUFFER) && id < ReceiveBuffers::count,
                "provided-buffer fixture lost selected input");
        return {owner_.receive_buffers.get(), static_cast<unsigned short>(id), 1};
    }

    Ring &owner_;
    std::array<int, 2> sockets_{-1, -1};
};

char payload(unsigned index)
{
    return static_cast<char>(index % 127 + 1);
}

void backlog_full_pool_and_ordered_reuse()
{
    Ring owner(1, true);
    SelectedInput input(owner);
    ReceiveBacklog backlog;
    RxChain first;
    RxChain second;
    std::array<bool, ReceiveBuffers::count> seen{};
    for (unsigned index = 0; index < ReceiveBuffers::count; ++index)
    {
        auto lease = input.receive(payload(index));
        require(!seen[lease.id()], "kernel selected a buffer still owned by the backlog");
        seen[lease.id()] = true;
        backlog.push(index % 2 ? second : first, std::move(lease));
        require(!lease, "backlog push left a second owner of the selected buffer");
    }
    require(first.count == ReceiveBuffers::count / 2 && second.count == first.count &&
                owner.receive_buffers->take_retry_credits(ReceiveBuffers::count) == 0,
            "full backlog lost leases or returned them before consumption");
    input.receive('z', true);
    for (unsigned index = 0; index < 256; ++index)
    {
        auto lease = backlog.pop(first);
        require(lease && lease.bytes().size() == 1 && lease.bytes().front() == payload(index * 2),
                "backlog reordered a connection's selected buffers");
    }
    backlog.clear(second);
    require(!second.count && second.head == RxChain::no_buffer &&
                second.tail == RxChain::no_buffer && !backlog.pop(second) &&
                owner.receive_buffers->take_retry_credits(ReceiveBuffers::count) == 768,
            "partial drain or per-connection clear leaked or duplicated buffer returns");
    // The byte submitted during ENOBUFS remains in the socket and must be received first
    auto reused = input.receive_queued();
    require(reused.bytes().front() == 'z', "buffer exhaustion discarded queued socket input");
    backlog.push(first, std::move(reused));
    for (unsigned index = 256; index < ReceiveBuffers::count / 2; ++index)
    {
        auto lease = backlog.pop(first);
        require(lease && lease.bytes().front() == payload(index * 2),
                "reused node overtook the connection's older input");
    }
    auto last = backlog.pop(first);
    require(last && last.bytes().front() == 'z' && !first.count && !backlog.pop(first),
            "reused backlog node lost its data or chain tail");
    last.release();
    require(owner.receive_buffers->take_retry_credits(ReceiveBuffers::count) == 257,
            "backlog reuse returned a selected buffer more or less than once");
    backlog.clear();
    require(owner.receive_buffers->take_retry_credits(ReceiveBuffers::count) == 0,
            "empty backlog teardown returned a buffer twice");
}
} // namespace

int main()
{
    ::alarm(15);
    try
    {
        ready_queue_bounds_and_wrap();
        backlog_full_pool_and_ordered_reuse();
        ::alarm(0);
        std::puts(
            "Bounded ready FIFO, full RX backlog, ordered reuse and exact buffer returns passed");
        return 0;
    }
    catch (const std::exception &error)
    {
        ::alarm(0);
        std::fprintf(stderr, "Pipeline ownership checks failed: %s\n", error.what());
        return 1;
    }
}
