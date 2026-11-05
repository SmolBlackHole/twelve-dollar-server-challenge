#include "completion.hpp"
#include "connection.hpp"
#include "ring.hpp"

#include <cassert>
#include <limits>
#include <utility>

namespace tds::net
{

Completion::~Completion()
{
    reset();
}

Completion::Completion(Completion &&other) noexcept
{
    *this = std::move(other);
}

Completion &Completion::operator=(Completion &&other) noexcept
{
    if (this != &other)
    {
        reset();
        kind = std::exchange(other.kind, CompletionKind::control);
        connection = std::exchange(other.connection, nullptr);
        source = std::exchange(other.source, nullptr);
        operation = std::exchange(other.operation, OperationId{});
        result = std::exchange(other.result, 0);
        flags = std::exchange(other.flags, 0);
        buffer = std::move(other.buffer);
#if TDS_TELEMETRY_LEVEL == 2
        serviced_ns = std::exchange(other.serviced_ns, 0);
#endif
        held_ = std::exchange(other.held_, nullptr);
        staged_ = std::exchange(other.staged_, nullptr);
    }

    return *this;
}

void Completion::hold(Connection &owner) noexcept
{
    assert(!held_ && owner.completion_refs < std::numeric_limits<std::uint16_t>::max());
    ++owner.completion_refs;
    held_ = &owner;
    connection = &owner;
}

void Completion::reset() noexcept
{
    // Return the selected buffer before the final pin can retire its connection
    buffer.release();
    connection = nullptr;
    source = nullptr;
    operation = {};
    kind = CompletionKind::control;
    result = 0;
    flags = 0;
#if TDS_TELEMETRY_LEVEL == 2
    serviced_ns = 0;
#endif
    if (auto *owner = std::exchange(held_, nullptr))
    {
        assert(owner->completion_refs);
        const auto key = owner->id();
        auto &ring = owner->ring;
        --owner->completion_refs;
        ring.try_retire(key);
    }
    if (auto *owner = std::exchange(staged_, nullptr))
    {
        assert(owner->staged_count);
        --owner->staged_count;
    }
}

void Completion::stage(Ring &owner) noexcept
{
    assert(!staged_);
    ++owner.staged_count;
    staged_ = &owner;
}

} // namespace tds::net
