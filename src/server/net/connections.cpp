#include "connections.hpp"
#include "../diagnostics/io.hpp"
#include "../http/exchange.hpp"

#include <cassert>
#include <stdexcept>
#include <utility>

namespace tds::net
{
namespace
{

unsigned checked_slot_count(unsigned slots)
{
    if (slots > OperationId::max_index + 1)
    {
        throw std::out_of_range("direct descriptor table exceeds operation slot range");
    }

    return slots;
}

} // namespace

Connection::Connection(Ring &owner, ConnectionKey key)
    : ring(owner), slot(key.slot), generation_(key.generation)
{
    OperationId::connection(Event::receive, key);
}

Connection::~Connection() = default;

ConnectionStore::ConnectionStore(unsigned slots)
    : records_(checked_slot_count(slots)), generations_(slots)
{
    free_slots_.reserve(slots);
    for (unsigned index = slots; index > 0; --index)
    {
        free_slots_.push_back(DirectSlot{index - 1});
    }
}

bool ConnectionStore::has_free_slot() const noexcept
{
    return !free_slots_.empty();
}

DirectSlot ConnectionStore::reserve()
{
    if (free_slots_.empty())
    {
        throw std::logic_error("no free direct descriptor slot");
    }
    const auto slot = free_slots_.back();
    free_slots_.pop_back();

    return slot;
}

void ConnectionStore::release(DirectSlot slot)
{
    assert(slot.value < records_.size() && !records_[slot.value]);
    free_slots_.push_back(slot);
}

Connection &ConnectionStore::create(Ring &owner, DirectSlot slot)
{
    assert(slot.value < records_.size() && !records_[slot.value]);
    auto connection =
        std::make_unique<Connection>(owner, ConnectionKey{slot, generations_[slot.value].value()});
    auto &record = *connection;
    records_[slot.value] = std::move(connection);
    telemetry::IoTelemetry::store_created(*this);

    return record;
}

void ConnectionStore::retire(DirectSlot slot)
{
    auto &record = records_.at(slot.value);
    if (!record || !record->retirable())
    {
        throw std::logic_error("retiring a connection before its lifetime ended");
    }
    record.reset();
    auto &generation = generations_[slot.value];
    if (generation.advance())
    {
        free_slots_.push_back(slot);
    }
    else
    {
        ++exhausted_count_;
    }
    telemetry::IoTelemetry::store_retired(*this);
}

void ConnectionStore::clear() noexcept
{
    records_.clear();
    generations_.clear();
    free_slots_.clear();
    exhausted_count_ = 0;
#if TDS_TELEMETRY_LEVEL >= 1
    live_count_ = 0;
#endif
}

Connection *ConnectionStore::find(ConnectionKey key) const noexcept
{
    if (key.slot.value >= records_.size())
    {
        return nullptr;
    }
    auto *record = records_[key.slot.value].get();

    return record && record->id() == key ? record : nullptr;
}

Connection *ConnectionStore::at(std::size_t index) const noexcept
{
    assert(index < records_.size());

    return records_[index].get();
}

std::size_t ConnectionStore::size() const noexcept
{
    return records_.size();
}

#if TDS_TELEMETRY_LEVEL >= 1
std::size_t ConnectionStore::live_count() const noexcept
{
    return live_count_;
}

std::size_t ConnectionStore::reserved_count() const noexcept
{
    return records_.size() - free_slots_.size() - live_count_ - exhausted_count_;
}
#endif

} // namespace tds::net
