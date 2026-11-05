#pragma once

#include "../telemetry/config.hpp"
#include "operation.hpp"
#include "work.hpp"

#include <cstdint>
#include <memory>
#include <stdexcept>

namespace tds::http
{
struct Exchange;
}

namespace tds::net
{

class Ring;

class IoState
{
  public:
    void start()
    {
        if (busy())
        {
            throw std::logic_error("starting I/O before its previous lifetime ended");
        }
        active_ = true;
        cancel_requested_ = false;
    }

    void complete(bool more = false)
    {
        if (!active_)
        {
            throw std::logic_error("completing an inactive I/O operation");
        }
        active_ = more;
    }

    bool request_cancel() noexcept
    {
        if (!active_ || cancel_requested_)
        {
            return false;
        }
        cancel_requested_ = true;
        cancel_pending_ = true;

        return true;
    }

    void acknowledge_cancel()
    {
        if (!cancel_pending_)
        {
            throw std::logic_error("acknowledging an unissued I/O cancellation");
        }
        cancel_pending_ = false;
    }

    bool active() const noexcept
    {
        return active_;
    }

    bool cancelling() const noexcept
    {
        return cancel_pending_;
    }

    bool cancel_requested() const noexcept
    {
        return cancel_requested_;
    }

    bool busy() const noexcept
    {
        return active_ || cancel_pending_;
    }

  private:
    // The original terminal CQE and an issued cancel acknowledgement may arrive in either order
    bool active_ = false;
    bool cancel_requested_ = false;
    bool cancel_pending_ = false;
};

struct AcceptTicket
{
    AcceptTicket() = default;
    AcceptTicket(const AcceptTicket &) = delete;
    AcceptTicket &operator=(const AcceptTicket &) = delete;

    Ring *ring = nullptr;
    DirectSlot slot{};
    bool pending = false;
    bool cancelling = false;
};

enum class RequestPhase : std::uint8_t
{
    reading,
    claimed,
    queued_write,
    responding
};

enum class ClosePhase : std::uint8_t
{
    open,
    requested,
    shutdown,
    close,
    complete
};

struct Connection
{
    Connection(Ring &owner, ConnectionKey key);
    ~Connection();
    Connection(const Connection &) = delete;
    Connection &operator=(const Connection &) = delete;

    ConnectionKey id() const noexcept
    {
        return {slot, generation_};
    }

    bool closing() const noexcept
    {
        return close_phase != ClosePhase::open;
    }

    bool retirable() const noexcept
    {
        return close_phase == ClosePhase::complete && !rx.busy() && !tx.busy() &&
               request_phase != RequestPhase::queued_write && !completion_refs && !db_refs &&
               !work_queued && !work_active && !input.count;
    }

    void claim_request()
    {
        if (request_phase != RequestPhase::reading)
        {
            throw std::logic_error("claiming a request already owned by application work");
        }
        request_phase = RequestPhase::claimed;
    }

    void start_queued_write()
    {
        if (request_phase != RequestPhase::claimed)
        {
            throw std::logic_error("queuing a write without its request claim");
        }
        request_phase = RequestPhase::queued_write;
    }

    void start_response()
    {
        if (request_phase == RequestPhase::responding)
        {
            throw std::logic_error("starting a response while one is already owned");
        }
        request_phase = RequestPhase::responding;
    }

    void finish_response()
    {
        if (request_phase != RequestPhase::responding)
        {
            throw std::logic_error("finishing a response without its request claim");
        }
        request_phase = RequestPhase::reading;
    }

    void abandon_request()
    {
        if (request_phase != RequestPhase::claimed && request_phase != RequestPhase::queued_write)
        {
            throw std::logic_error("abandoning a request outside pending application work");
        }
        request_phase = RequestPhase::reading;
    }

    Ring &ring;
    std::unique_ptr<http::Exchange> exchange;
#if TDS_TELEMETRY_LEVEL == 2
    // First ENOBUFS service, retained across retries until a successful receive CQE
    std::uint64_t receive_enobufs_since_ns = 0;
#endif
    const DirectSlot slot;
    IoState rx;
    IoState tx;
    RxChain input;
    WorkReason work_reasons = WorkReason::none;
    std::uint16_t completion_refs = 0;
    std::uint16_t db_refs = 0;
    RequestPhase request_phase = RequestPhase::reading;
    ClosePhase close_phase = ClosePhase::open;
    bool rx_eof = false;
    bool receive_deferred = false;
    bool work_queued = false;
    bool work_active = false;
    bool send_ready = false;
    bool rx_paused = false;
    bool close_after = false;

  private:
    const std::uint64_t generation_;
};

} // namespace tds::net
