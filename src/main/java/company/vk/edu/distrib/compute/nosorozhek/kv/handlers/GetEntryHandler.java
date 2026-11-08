package company.vk.edu.distrib.compute.nosorozhek.kv.handlers;

import com.sun.net.httpserver.HttpExchange;
import company.vk.edu.distrib.compute.nosorozhek.kv.ReplicationCoordinator;

import java.io.IOException;
import java.util.Optional;

public class GetEntryHandler implements Handler {
    private final ReplicationCoordinator coordinator;

    public GetEntryHandler(ReplicationCoordinator coordinator) {
        this.coordinator = coordinator;
    }

    @Override
    public void handle(HttpExchange exchange, RouteParameters parameters) throws IOException {
        String entryId = RouteParameters.requiredId(exchange);

        boolean internal = ReplicationCoordinator.isInternalRequest(exchange);
        int ack;
        if (internal) {
            ack = 1;
        } else {
            ack = RouteParameters.optionalAck(exchange).orElse(1);
        }

        Optional<byte[]> entry = coordinator.get(entryId, ack, internal);

        if (entry.isEmpty()) {
            exchange.sendResponseHeaders(404, -1);
            exchange.close();
            return;
        }

        exchange.getResponseHeaders().set("Content-Type", "text/html; charset=utf-8");
        exchange.sendResponseHeaders(200, entry.get().length);
        exchange.getResponseBody().write(entry.get());
        exchange.close();
    }
}
