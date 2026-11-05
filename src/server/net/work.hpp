#pragma once

#include "buffers.hpp"
#include "operation.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <stdexcept>
#include <utility>

namespace tds::net
{

enum class WorkReason : std::uint8_t
{
    none = 0,
    input = 1,
    transmit = 2,
    resume = 4,
    close = 8
};

constexpr WorkReason operator|(WorkReason left, WorkReason right) noexcept
{
    return static_cast<WorkReason>(static_cast<std::uint8_t>(left) |
                                   static_cast<std::uint8_t>(right));
}

constexpr WorkReason &operator|=(WorkReason &left, WorkReason right) noexcept
{
    left = left | right;

    return left;
}

class ReadyQueue
{
  public:
    static constexpr std::size_t capacity = 256;

    bool push(ConnectionKey key) noexcept
    {
        if (!remaining())
        {
            return false;
        }
        keys_[(head_ + size_) % capacity] = key;
        ++size_;

        return true;
    }

    std::optional<ConnectionKey> pop() noexcept
    {
        if (empty())
        {
            return std::nullopt;
        }
        const auto key = keys_[head_];
        head_ = (head_ + 1) % capacity;
        --size_;

        return key;
    }

    std::size_t size() const noexcept
    {
        return size_;
    }

    std::size_t remaining() const noexcept
    {
        return capacity - size_;
    }

    bool empty() const noexcept
    {
        return !size_;
    }

    void clear() noexcept
    {
        head_ = 0;
        size_ = 0;
    }

  private:
    std::array<ConnectionKey, capacity> keys_{};
    std::size_t head_ = 0;
    std::size_t size_ = 0;
};

struct RxChain
{
    static constexpr std::uint16_t no_buffer = std::numeric_limits<std::uint16_t>::max();

    std::uint16_t head = no_buffer;
    std::uint16_t tail = no_buffer;
    std::uint16_t count = 0;
};

class ReceiveBacklog
{
  public:
    // Chains and exclusively owned selected leases must belong to this ring
    void push(RxChain &chain, ReceiveBufferLease lease)
    {
        if (!lease)
        {
            throw std::invalid_argument("cannot queue an empty receive lease");
        }
        const auto id = lease.id();
        if (id >= nodes_.size())
        {
            throw std::out_of_range("receive buffer index exceeds backlog storage");
        }
        auto &node = nodes_[id];
        if (node.lease)
        {
            throw std::logic_error("receive buffer already belongs to a backlog");
        }
        node.lease = std::move(lease);
        node.next = RxChain::no_buffer;
        if (chain.count)
        {
            nodes_[chain.tail].next = id;
        }
        else
        {
            chain.head = id;
        }
        chain.tail = id;
        ++chain.count;
    }

    ReceiveBufferLease pop(RxChain &chain) noexcept
    {
        if (!chain.count)
        {
            return {};
        }
        auto &node = nodes_[chain.head];
        chain.head = node.next;
        --chain.count;
        if (!chain.count)
        {
            chain.tail = RxChain::no_buffer;
        }
        node.next = RxChain::no_buffer;

        return std::move(node.lease);
    }

    void clear(RxChain &chain) noexcept
    {
        while (chain.count)
        {
            pop(chain).release();
        }
    }

    // Teardown invalidates all chains before the provided-buffer pool is destroyed
    void clear() noexcept
    {
        for (auto &node : nodes_)
        {
            node.lease.release();
            node.next = RxChain::no_buffer;
        }
    }

  private:
    struct Node
    {
        ReceiveBufferLease lease;
        std::uint16_t next = RxChain::no_buffer;
    };

    std::array<Node, ReceiveBuffers::count> nodes_;
};

} // namespace tds::net
