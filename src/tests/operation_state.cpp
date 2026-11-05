#include "net/connection.hpp"
#include "net/operation.hpp"

#include <array>
#include <cstdint>
#include <cstdio>
#include <exception>
#include <stdexcept>
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

void operation_identity()
{
    static_assert(sizeof(OperationId) == sizeof(std::uint64_t));
    require(OperationId{}.raw() == 0 && OperationId{}.event() == Event::none &&
                !OperationId{}.is_connection(),
            "empty operation decoded as a connection");
    constexpr std::array connection_events{Event::receive,        Event::send,
                                           Event::shutdown,       Event::close,
                                           Event::cancel_receive, Event::cancel_send};
    for (const auto event : connection_events)
    {
        for (const auto key :
             {ConnectionKey{DirectSlot{0}, 1},
              ConnectionKey{DirectSlot{OperationId::max_index}, OperationId::max_generation}})
        {
            const auto operation = OperationId::connection(event, key);
            const auto decoded = OperationId::from_raw(operation.raw());
            require(decoded.event() == event && decoded.is_connection() &&
                        decoded.index() == key.slot.value &&
                        decoded.generation() == key.generation && decoded.connection_key() == key,
                    "connection operation lost kind, slot or high generation bits");
            require((operation.raw() & 15U) == static_cast<std::uint64_t>(event) &&
                        ((operation.raw() >> 4) & 65535U) == key.slot.value &&
                        (operation.raw() >> 20) == key.generation,
                    "operation tag changed its documented 4/16/44-bit layout");
        }
        rejects(
            [&] {
                OperationId::connection(event, {DirectSlot{0}, 0});
            },
            "connection tag accepted generation zero");
        rejects(
            [&] {
                OperationId::connection(event, {DirectSlot{0}, OperationId::max_generation + 1});
            },
            "connection tag truncated an overflowing generation");
        rejects(
            [&] {
                OperationId::connection(event, {DirectSlot{OperationId::max_index + 1}, 1});
            },
            "connection tag truncated an overflowing slot");
        rejects(
            [&] {
                OperationId::control(event);
            },
            "connection operation was accepted as a control");
    }

    constexpr std::array control_events{Event::accept, Event::stop, Event::wake,
                                        Event::cancel_accept, Event::cancel_wake};
    for (const auto event : control_events)
    {
        for (const auto index : {0U, OperationId::max_index})
        {
            const auto operation = OperationId::control(event, index);
            const auto decoded = OperationId::from_raw(operation.raw());
            require(decoded.event() == event && decoded.index() == index &&
                        decoded.generation() == 0 && !decoded.is_connection(),
                    "control tag decoded as a connection generation");
        }
        rejects(
            [&] {
                OperationId::connection(event, {DirectSlot{1}, 1});
            },
            "control identity was accepted as a connection");
        rejects(
            [&] {
                OperationId::control(event, OperationId::max_index + 1);
            },
            "control tag truncated its index");
    }
    rejects(
        [] {
            OperationId::connection(Event::none, {DirectSlot{0}, 1});
        },
        "empty identity accepted connection ownership");
    rejects(
        [] {
            OperationId::control(Event::none, 1);
        },
        "empty identity accepted a nonzero index");
    rejects(
        [] {
            OperationId::control(static_cast<Event>(15));
        },
        "control factory accepted an undefined event");
    rejects(
        [] {
            OperationId::connection(static_cast<Event>(15), {DirectSlot{0}, 1});
        },
        "connection factory accepted an undefined event");
    require(OperationId::control(Event::none).raw() == 0,
            "explicit empty control did not retain the zero identity");
    const auto first = OperationId::connection(Event::receive, {DirectSlot{7}, 1});
    const auto reused = OperationId::connection(Event::receive, {DirectSlot{7}, 2});
    const auto cancel = OperationId::connection(Event::cancel_receive, {DirectSlot{7}, 1});
    require(first.raw() != reused.raw() && first.raw() != cancel.raw() &&
                first.connection_key() == cancel.connection_key(),
            "slot reuse or cancellation aliased an original operation identity");
}

void operation_lifetime()
{
    IoState state;
    require(!state.active() && !state.cancelling() && !state.busy(),
            "new I/O state owns a kernel operation");
    require(!state.request_cancel(), "idle operation requested a cancellation acknowledgement");
    rejects(
        [&] {
            state.complete();
        },
        "inactive operation accepted a terminal completion");
    rejects(
        [&] {
            state.acknowledge_cancel();
        },
        "unissued cancellation accepted an acknowledgement");
    state.start();
    rejects(
        [&] {
            state.start();
        },
        "active operation allowed rearm");
    state.complete(true);
    require(state.active() && state.busy(), "MORE retired the original operation");
    state.complete();
    require(!state.busy() && !state.cancelling(),
            "natural terminal completion invented a cancel acknowledgement");
    state.start();
    state.complete();
    require(!state.busy(), "naturally completed operation could not be reused");

    for (const bool acknowledgement_first : {false, true})
    {
        state.start();
        require(state.request_cancel() && state.cancelling(),
                "active cancellation did not retain its acknowledgement");
        require(!state.request_cancel(), "duplicate cancellation requested a second SQE");
        state.complete(true);
        require(state.active() && state.cancelling(), "MORE cleared a cancellation lifetime");
        if (acknowledgement_first)
        {
            state.acknowledge_cancel();
            require(state.active() && !state.cancelling() && state.busy(),
                    "cancel acknowledgement retired its still-active original");
            require(state.cancel_requested() && !state.request_cancel(),
                    "acknowledgement allowed a second cancellation of the same original");
            rejects(
                [&] {
                    state.start();
                },
                "acknowledgement allowed rearm before terminal");
            state.complete();
        }
        else
        {
            state.complete();
            require(!state.active() && state.cancelling() && state.busy(),
                    "original terminal forgot its pending cancellation acknowledgement");
            rejects(
                [&] {
                    state.start();
                },
                "terminal allowed rearm before acknowledgement");
            state.acknowledge_cancel();
        }
        require(!state.busy(), "cancel pair retained ownership after both completions");
    }
    state.start();
    require(!state.cancel_requested() && state.request_cancel(),
            "new original retained the previous cancellation request");
    state.complete();
    state.acknowledge_cancel();

    IoState receive;
    IoState send;
    receive.start();
    send.start();
    receive.complete(true);
    send.complete();
    require(receive.active() && !send.busy(), "TX completion changed independent RX lifetime");
    send.start();
    require(receive.request_cancel(), "simultaneous RX could not be cancelled");
    receive.complete();
    receive.acknowledge_cancel();
    require(!receive.busy() && send.active(), "RX cancellation changed independent TX lifetime");
    send.complete();
}

void generation_exhaustion()
{
    SlotGeneration generation;
    require(generation.value() == 1 && generation.advance() && generation.value() == 2,
            "slot generation did not begin at one and advance once");
    SlotGeneration last(OperationId::max_generation - 1);
    require(last.advance() && last.value() == OperationId::max_generation,
            "last representable slot generation was not usable");
    require(!last.advance() && last.value() == OperationId::max_generation && !last.advance() &&
                last.value() == OperationId::max_generation,
            "exhausted slot generation wrapped or became reusable");
    rejects(
        [] {
            SlotGeneration invalid(0);
        },
        "slot generation accepted zero");
    rejects(
        [] {
            SlotGeneration invalid(OperationId::max_generation + 1);
        },
        "slot generation accepted a value outside its tag");
}
} // namespace

int main()
{
    try
    {
        operation_identity();
        operation_lifetime();
        generation_exhaustion();
        std::puts("Operation tags, MORE, independent RX/TX and cancellation ordering passed");
        return 0;
    }
    catch (const std::exception &error)
    {
        std::fprintf(stderr, "Operation state checks failed: %s\n", error.what());
        return 1;
    }
}
