#include "server.hpp"
#include "app/application.hpp"
#include "diagnostics/io.hpp"
#include "http/exchange.hpp"
#include "net/reactor.hpp"

#include <array>
#include <iostream>
#include <span>
#include <stdexcept>
#include <utility>
#include <vector>

namespace tds
{
namespace
{
using net::Completion;
using net::CompletionKind;
using net::Connection;
using net::Reactor;
using net::WorkReason;
} // namespace

class Server::State
{
  public:
    State(std::string_view host, std::uint16_t port, std::string database_path, std::string secret)
        : application(std::move(database_path), std::move(secret)), reactor(host, port)
    {
    }

    void request_stop() const noexcept
    {
        reactor.request_stop();
    }

    [[nodiscard]] int stop_descriptor() const noexcept
    {
        return reactor.stop_descriptor();
    }

    [[nodiscard]] std::uint16_t port() const noexcept
    {
        return reactor.port();
    }

    void run()
    {
        const auto report_on_exit = telemetry.run(reactor);
        reactor.start();
        while (reactor.has_work() || !pending_writes.commands.empty())
        {
            if (!reactor.has_work())
            {
                flush_writes();
                if (!reactor.has_work())
                {
                    break;
                }
            }
            reactor.begin_turn();
            for (const auto &owner : reactor.rings())
            {
                run_work(*owner);
            }
            while (true)
            {
                auto completions = reactor.collect();
                if (completions.empty())
                {
                    break;
                }
                auto &owner = *completions.front().source;
                for (auto &completion : completions)
                {
                    ingest(completion);
                }
                // Transfer every result and lease before releasing completion lifetime pins
                for (auto &completion : completions)
                {
                    completion.reset();
                }
                run_work(owner);
            }
            for (const auto &owner : reactor.rings())
            {
                run_work(*owner);
            }
            // Flush before the reactor waits so a partial batch cannot strand its replies
            flush_writes();
            reactor.finish_turn();
            telemetry.report(reactor, false);
        }
    }

  private:
    void dispatch(Connection &connection)
    {
        connection.claim_request();
        const auto &session = connection.exchange->session;
        auto sample = telemetry.handler(connection);
        auto prepared = application.prepare(session.accepted_head(), session.message().body);
        if (!prepared)
        {
            sample.finish();
            connection.close_after =
                connection.close_after || prepared.error().status == Status::internal_error;
            respond(connection, Application::error_response(prepared.error()));

            return;
        }
        if (auto *write = std::get_if<WriteCommand>(&*prepared))
        {
            queue_write(connection, std::move(*write));

            return;
        }
        auto response = application.read(std::get<ReadQuery>(*prepared),
                                         connection.exchange->reply.take_body());
        sample.finish();
        respond(connection, std::move(response));
    }

    void send(Connection &connection)
    {
        connection.send_ready = false;
        telemetry.send_prepared(connection);
        auto &message = connection.exchange->reply.message();
        reactor.send(connection, message);
    }

    void respond(Connection &connection, Response response)
    {
        connection.close_after =
            connection.close_after || !connection.exchange->session.message().keep_alive;
        connection.exchange->reply.prepare(std::move(response), connection.close_after);
        connection.start_response();
        connection.send_ready = true;
    }

    void flush_writes()
    {
        if (pending_writes.commands.empty() || pending_writes.resolved)
        {
            return;
        }
        const auto count = pending_writes.commands.size();
        const auto connections = std::span(pending_writes.connections).first(count);
        if (reactor.stopping())
        {
            for (auto *connection : connections)
            {
                connection->abandon_request();
                close(*connection);
            }
            pending_writes.clear();

            return;
        }
        const auto responses = std::span(pending_writes.responses).first(count);
        const auto outcomes = std::span(pending_writes.outcomes).first(count);
        try
        {
            const auto sample = telemetry.write_batch(count);
            const auto committed = application.commit(pending_writes.commands, outcomes);
            // Serialization or send failures must never replay already committed commands
            pending_writes.resolved = true;
            if (!committed)
            {
                throw std::runtime_error(committed.error().message);
            }
            for (std::size_t index = 0; index < count; ++index)
            {
                if (connections[index]->closing())
                {
                    continue;
                }
                try
                {
                    responses[index] =
                        application.write_reply(pending_writes.commands[index], outcomes[index],
                                                connections[index]->exchange->reply.take_body());
                }
                catch (const std::exception &error)
                {
                    std::cerr << "request failed: " << error.what() << '\n';
                    responses[index] = Application::error_response(
                        {Status::internal_error, "internal server error"});
                }
            }
        }
        catch (const std::exception &error)
        {
            std::cerr << "write batch failed: " << error.what() << '\n';
            pending_writes.resolved = true;
            for (auto &response : responses)
            {
                response =
                    Application::error_response({Status::internal_error, "internal server error"});
            }
        }
        // Queued commands and their targets stay alive until replies have been staged
        for (std::size_t index = 0; index < count; ++index)
        {
            auto &connection = *connections[index];
            if (connection.closing())
            {
                connection.abandon_request();
                continue;
            }
            connection.close_after =
                connection.close_after || responses[index].status == Status::internal_error;
            respond(connection, std::move(responses[index]));
            // This bounded response group is the batch's TX stage and needs no extra tickets
            send(connection);
        }
        pending_writes.clear();
    }

    void queue_write(Connection &connection, WriteCommand command)
    {
        const auto index = pending_writes.commands.size();
        pending_writes.commands.push_back(std::move(command));
        pending_writes.connections[index] = &connection;
        ++connection.db_refs;
        connection.start_queued_write();
    }

    void process(Connection &connection)
    {
        auto &exchange = *connection.exchange;
        auto &session = exchange.session;
        try
        {
            bool may_feed = true;
            while (true)
            {
                const auto result = session.consume();
                if (!result.valid)
                {
                    connection.close_after = true;
                    respond(connection, Application::error_response(
                                            {session.error(), "malformed HTTP request"}));

                    return;
                }
                if (session.headers_pending())
                {
                    const auto &message = session.message();
                    auto accepted = application.accept_headers(
                        {message.method, message.path, message.authorization});
                    if (!accepted)
                    {
                        // Unread body bytes must not become the next keep-alive request
                        connection.close_after = true;
                        respond(connection, Application::error_response(accepted.error()));

                        return;
                    }
                    session.accept_headers(std::move(*accepted));
                    continue;
                }
                if (session.complete())
                {
                    dispatch(connection);
                }
                else if (may_feed && receive_input(connection))
                {
                    may_feed = false;
                    continue;
                }
                return;
            }
        }
        catch (const std::exception &error)
        {
            if (connection.request_phase == net::RequestPhase::responding)
            {
                // A failed send cannot become a second response or replay an accepted write
                throw;
            }
            std::cerr << "request failed: " << error.what() << '\n';
            connection.close_after = true;
            respond(connection,
                    Application::error_response({Status::internal_error, "internal server error"}));
        }
    }

    void close(Connection &connection)
    {
        connection.ring.backlog.clear(connection.input);
        connection.send_ready = false;
        if (!connection.closing())
        {
            reactor.close(connection);
        }
    }

    void reduce_send(const Completion &completion)
    {
        auto &connection = *completion.connection;
        const auto result = completion.result;
        telemetry.send_completed(completion);
        // Fatal and shutdown completions have already started closing in the reactor
        if (connection.closing())
        {
            return;
        }
        if (result < 0)
        {
            connection.send_ready = true;
            return;
        }
        auto &exchange = *connection.exchange;
        if (!exchange.reply.advance(static_cast<std::size_t>(result)))
        {
            connection.send_ready = true;
            return;
        }
        if (connection.close_after)
        {
            close(connection);
            return;
        }
        exchange.reply.reset();
        exchange.session.reset();
        connection.finish_response();
        // EOF is reduced before ingestion, so later records can still carry ordered input
        if (!exchange.session.has_input() && !connection.input.count && !connection.rx_eof)
        {
            connection.ring.recycle_exchange(std::move(connection.exchange));
        }
    }

    void schedule(Connection &connection, WorkReason reason)
    {
        if (!reactor.schedule(connection, reason))
        {
            // CQ reservation and active-ticket ownership leave room for every new ticket
            throw std::logic_error("ready queue capacity contract violated");
        }
    }

    void schedule_if_ready(Connection &connection)
    {
        if (connection.closing() || reactor.stopping())
        {
            return;
        }
        auto reasons = WorkReason::none;
        if (connection.send_ready && !connection.tx.busy())
        {
            reasons |= WorkReason::transmit;
        }
        if (connection.request_phase == net::RequestPhase::reading &&
            (connection.input.count || connection.rx_eof ||
             (connection.exchange && connection.exchange->session.has_input())))
        {
            reasons |= WorkReason::input;
        }
        if (connection.rx_paused && !connection.input.count && !connection.rx.busy() &&
            !connection.rx_eof && !connection.receive_deferred)
        {
            reasons |= WorkReason::resume;
        }
        if (reasons != WorkReason::none)
        {
            schedule(connection, reasons);
        }
    }

    void ingest(Completion &completion)
    {
        if (!completion.connection)
        {
            return;
        }
        auto &connection = *completion.connection;
        if (completion.kind == CompletionKind::sent)
        {
            reduce_send(completion);
        }
        if (connection.closing() || reactor.stopping())
        {
            close(connection);
            return;
        }
        if (completion.kind == CompletionKind::received && completion.result > 0)
        {
            connection.ring.backlog.push(connection.input, std::move(completion.buffer));
            connection.work_reasons |= WorkReason::input;
            if (connection.input.count >= 2 && !connection.rx_paused)
            {
                connection.rx_paused = true;
                reactor.cancel_receive(connection);
            }
        }
        schedule_if_ready(connection);
    }

    bool receive_input(Connection &connection)
    {
        if (!connection.exchange)
        {
            if (!connection.input.count)
            {
                return false;
            }
            connection.exchange = connection.ring.acquire_exchange();
        }
        auto &session = connection.exchange->session;
        if (session.has_input() || !connection.input.count || connection.ring.rx_slices == 256)
        {
            return false;
        }
        auto lease = connection.ring.backlog.pop(connection.input);
        ++connection.ring.rx_slices;
        const auto received = telemetry.receive(connection, lease);
        received.attach(connection);
        const auto input = lease.bytes();
        session.append({input.data(), input.size()});

        return true;
    }

    void finish_work(Connection &connection)
    {
        if (connection.closing() || reactor.stopping())
        {
            close(connection);
        }
        else
        {
            const bool tail = connection.exchange && connection.exchange->session.has_input();
            if (connection.request_phase == net::RequestPhase::reading && !tail &&
                !connection.input.count && connection.rx_eof)
            {
                close(connection);
            }
            else if (connection.rx_paused && !connection.input.count && !connection.rx.busy())
            {
                reactor.resume_receive(connection);
            }
        }
        connection.work_active = false;
        connection.work_reasons =
            connection.input.count ||
                    (connection.exchange && connection.exchange->session.has_input())
                ? WorkReason::input
                : WorkReason::none;
        schedule_if_ready(connection);
        connection.ring.try_retire(connection.id());
    }

    void run_work(net::Ring &owner)
    {
        auto &active = active_work[owner.index];
        std::size_t count = 0;
        while (owner.work_visits < 256 && count < active.size())
        {
            const auto key = owner.ready.pop();
            if (!key)
            {
                break;
            }
            auto *connection = owner.connections.find(*key);
            if (!connection)
            {
                continue;
            }
            connection->work_queued = false;
            connection->work_active = true;
            active[count++] = *key;
            ++owner.work_visits;
        }
        const auto keys = std::span(active).first(count);
        for (const auto key : keys)
        {
            auto &connection = *owner.connections.find(key);
            if (connection.closing() || reactor.stopping())
            {
                close(connection);
            }
            else if (connection.request_phase == net::RequestPhase::reading)
            {
                receive_input(connection);
            }
        }
        for (const auto key : keys)
        {
            auto &connection = *owner.connections.find(key);
            if (!connection.closing() && !reactor.stopping() && connection.exchange &&
                connection.request_phase == net::RequestPhase::reading)
            {
                process(connection);
                if (pending_writes.commands.size() == write_batch_limit)
                {
                    flush_writes();
                }
            }
        }
        for (const auto key : keys)
        {
            auto &connection = *owner.connections.find(key);
            if (!connection.closing() && !reactor.stopping() && connection.send_ready &&
                !connection.tx.busy())
            {
                send(connection);
            }
            finish_work(connection);
        }
    }

    // Keep the application alive until reactor-owned I/O storage has retired
    Application application;
    Reactor reactor;
    std::array<std::array<net::ConnectionKey, net::ReadyQueue::capacity>,
               telemetry::io_ring_capacity>
        active_work{};

    struct WriteBatch
    {
        WriteBatch()
        {
            commands.reserve(write_batch_limit);
        }

        ~WriteBatch()
        {
            clear();
        }

        void clear() noexcept
        {
            // Keep the slots, but release previous results and error strings after publishing
            const auto count = commands.size();
            for (auto &outcome : std::span(outcomes).first(count))
            {
                outcome.emplace();
            }
            commands.clear();
            for (std::size_t index = 0; index < count; ++index)
            {
                responses[index] = Response{};
                auto &connection = *connections[index];
                connections[index] = nullptr;
                --connection.db_refs;
                connection.ring.try_retire(connection.id());
            }
            resolved = false;
        }

        std::array<Connection *, write_batch_limit> connections{};
        std::array<Response, write_batch_limit> responses{};
        std::array<WriteOutcome, write_batch_limit> outcomes{};
        std::vector<WriteCommand> commands;
        bool resolved = false;
    } pending_writes;

    [[no_unique_address]] telemetry::IoTelemetry telemetry;
};

Server::Server(std::string_view host, std::uint16_t port, std::string database_path,
               std::string secret)
    : state_(std::make_unique<State>(host, port, std::move(database_path), std::move(secret)))
{
}

Server::~Server() = default;

void Server::run()
{
    state_->run();
}

void Server::request_stop() const noexcept
{
    state_->request_stop();
}

int Server::stop_descriptor() const noexcept
{
    return state_->stop_descriptor();
}

std::uint16_t Server::port() const noexcept
{
    return state_->port();
}

} // namespace tds
