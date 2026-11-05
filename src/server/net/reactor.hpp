#pragma once

#include "../telemetry/config.hpp"
#include "completion.hpp"
#include "ring.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <string_view>
#include <sys/socket.h>
#include <vector>

namespace tds::net
{

class Reactor
{
  public:
    // Create, drive and destroy the reactor on the same issuer thread
    Reactor(std::string_view host, std::uint16_t port);
    ~Reactor();
    Reactor(const Reactor &) = delete;
    Reactor &operator=(const Reactor &) = delete;

    void start();
    unsigned pending_count() const noexcept;
    bool has_work() const noexcept;
    void begin_turn();
    // Consume the returned completion before next advances or retires another record
    bool next(Completion &completion);
    // Consume and reset every record before collecting another bounded ring prefix
    std::span<Completion> collect();
    bool schedule(Connection &connection, WorkReason reason) noexcept;
    void resume_receive(Connection &connection);
    void finish_turn();

    bool stopping() const noexcept;
    void receive(Connection &connection);
    void send(Connection &connection, msghdr &message);
    void cancel_receive(Connection &connection);
    void cancel_send(Connection &connection);
    void close(Connection &connection);
    void request_stop() const noexcept;
    int stop_descriptor() const noexcept;
    std::uint16_t port() const noexcept;
    std::span<const std::unique_ptr<Ring>> rings() const noexcept;

  private:
    void receive(Connection &connection, bool poll_first);
    void begin_ring(Ring &owner);
    void consume(Ring &owner, Completion &completion);
    void accept();
    void arm_wake(Ring &owner);
    void retry_receives(Ring &owner);
    void cancel(Ring &owner, OperationId original, OperationId acknowledgement);
    void advance_close(Connection &connection);
    void stop_all();
    void advance_shutdown(Ring &owner);
    void complete_wake(Ring &owner, int result);
    void complete_accept(AcceptTicket &ticket, int result);
    void complete_shutdown(Connection &connection, int result);
    void complete_close(Ring &owner, Connection &connection, int result);
    void complete_io(Connection &connection, Event event, int result, unsigned flags,
                     ReceiveBufferLease buffer, Completion &completion);
    void complete(Ring &owner, OperationId operation, int result, unsigned flags,
                  Completion &completion);
    int wait();
    void finish_ring();
    void finish_wakeups();

    struct CqeCursor
    {
        io_uring_cqe *cqes = nullptr;
        unsigned mask = 0;
        unsigned head = 0;
        unsigned tail = 0;
    };

    Descriptor listener_;
    Descriptor stop_;
    static constexpr std::size_t max_rings = telemetry::io_ring_capacity;
    // Ticket indices stay stable until original accepts and issued cancel ACKs have completed
    std::array<AcceptTicket, 64> accept_tickets_;
    std::vector<std::unique_ptr<Ring>> rings_;
    unsigned table_size_ = 0;
    std::uint16_t port_ = 0;
    bool stopping_ = false;
    bool closing_wakeups_ = false;
    unsigned previous_count_ = 0;
    bool accepted_ = false;
    unsigned turn_count_ = 0;
    std::size_t ring_index_ = 0;
    CqeCursor cq_cursor_{};
    unsigned drained_ = 0;
    bool ring_started_ = false;
};

} // namespace tds::net
