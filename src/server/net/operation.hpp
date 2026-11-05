#pragma once

#include <cstdint>
#include <stdexcept>

namespace tds::net
{

struct DirectSlot
{
    unsigned value;

    bool operator==(const DirectSlot &) const = default;
};

struct ConnectionKey
{
    DirectSlot slot;
    std::uint64_t generation;

    bool operator==(const ConnectionKey &) const = default;
};

enum class Event : std::uint8_t
{
    none = 0,
    accept = 1,
    receive = 2,
    send = 3,
    shutdown = 4,
    close = 5,
    stop = 6,
    wake = 7,
    cancel_receive = 8,
    cancel_send = 9,
    cancel_accept = 10,
    cancel_wake = 11
};

class OperationId
{
  public:
    // Kind occupies bits 0-3, slot/control index 4-19, generation 20-63
    // Generation zero identifies controls; connection generations never wrap
    // RX/TX cancellation acknowledgements retain their original connection generation
    static constexpr unsigned max_index = (1U << 16) - 1;
    static constexpr std::uint64_t max_generation = (std::uint64_t{1} << 44) - 1;

    constexpr OperationId() noexcept = default;

    static constexpr OperationId connection(Event event, ConnectionKey key)
    {
        if (!connection_event(event))
        {
            throw std::invalid_argument("operation kind does not identify a connection");
        }
        if (key.slot.value > max_index || key.generation > max_generation)
        {
            throw std::out_of_range("connection operation identity exceeds its bit range");
        }
        if (!key.generation)
        {
            throw std::invalid_argument("connection generation must be nonzero");
        }

        return from_raw((key.generation << 20) | (std::uint64_t{key.slot.value} << 4) |
                        static_cast<std::uint64_t>(event));
    }

    static constexpr OperationId control(Event event, unsigned index = 0)
    {
        if (!control_event(event) || (event == Event::none && index))
        {
            throw std::invalid_argument("operation kind does not identify this control");
        }
        if (index > max_index)
        {
            throw std::out_of_range("control operation index exceeds its bit range");
        }

        return from_raw((std::uint64_t{index} << 4) | static_cast<std::uint64_t>(event));
    }

    static constexpr OperationId from_raw(std::uint64_t value) noexcept
    {
        OperationId identity;
        identity.value_ = value;

        return identity;
    }

    constexpr std::uint64_t raw() const noexcept
    {
        return value_;
    }

    constexpr Event event() const noexcept
    {
        return static_cast<Event>(value_ & 15);
    }

    constexpr unsigned index() const noexcept
    {
        return static_cast<unsigned>((value_ >> 4) & max_index);
    }

    constexpr std::uint64_t generation() const noexcept
    {
        return value_ >> 20;
    }

    constexpr ConnectionKey connection_key() const
    {
        if (!is_connection())
        {
            throw std::logic_error("control operation has no connection key");
        }

        return {DirectSlot{index()}, generation()};
    }

    constexpr bool is_connection() const noexcept
    {
        return generation() != 0 && connection_event(event());
    }

    bool operator==(const OperationId &) const = default;

  private:
    static constexpr bool connection_event(Event event) noexcept
    {
        return event == Event::receive || event == Event::send || event == Event::shutdown ||
               event == Event::close || event == Event::cancel_receive ||
               event == Event::cancel_send;
    }

    static constexpr bool control_event(Event event) noexcept
    {
        return event == Event::none || event == Event::accept || event == Event::stop ||
               event == Event::wake || event == Event::cancel_accept || event == Event::cancel_wake;
    }

    std::uint64_t value_ = 0;
};

class SlotGeneration
{
  public:
    constexpr SlotGeneration() noexcept = default;

    explicit constexpr SlotGeneration(std::uint64_t value) : value_(value)
    {
        if (!value)
        {
            throw std::invalid_argument("slot generation must be nonzero");
        }
        if (value > OperationId::max_generation)
        {
            throw std::out_of_range("slot generation exceeds operation bit range");
        }
    }

    constexpr std::uint64_t value() const noexcept
    {
        return value_;
    }

    constexpr bool advance() noexcept
    {
        if (value_ == OperationId::max_generation)
        {
            return false;
        }
        ++value_;

        return true;
    }

  private:
    std::uint64_t value_ = 1;
};

} // namespace tds::net
