#pragma once

#include "../telemetry/config.hpp"
#include "buffers.hpp"
#include "operation.hpp"

#include <cstdint>

namespace tds::net
{

struct Connection;
class Ring;
class Reactor;

enum class CompletionKind
{
    control,
    received,
    sent
};

// A buffered CQE owns its lease and pins its connection until consumption or discard
// Its ring must outlive the record, including during fatal stack unwinding
class Completion
{
  public:
    Completion() = default;
    ~Completion();
    Completion(const Completion &) = delete;
    Completion &operator=(const Completion &) = delete;
    Completion(Completion &&other) noexcept;
    Completion &operator=(Completion &&other) noexcept;

    void reset() noexcept;

    CompletionKind kind = CompletionKind::control;
    Connection *connection = nullptr;
    Ring *source = nullptr;
    OperationId operation;
    int result = 0;
    unsigned flags = 0;
    ReceiveBufferLease buffer;
#if TDS_TELEMETRY_LEVEL == 2
    std::uint64_t serviced_ns = 0;
#endif

  private:
    friend class Reactor;
    void hold(Connection &owner) noexcept;
    void stage(Ring &owner) noexcept;
    Connection *held_ = nullptr;
    Ring *staged_ = nullptr;
};

} // namespace tds::net
