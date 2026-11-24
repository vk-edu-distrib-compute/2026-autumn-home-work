package company.vk.edu.distrib.compute.nosorozhek.kv.handlers;

import com.sun.net.httpserver.HttpExchange;
import company.vk.edu.distrib.compute.nosorozhek.kv.ReplicationCoordinator;

import java.io.IOException;

public class PutEntryHandler implements Handler {
    private static final int BODY_SIZE_LIMIT = 8192;

    private final ReplicationCoordinator coordinator;

    public PutEntryHandler(ReplicationCoordinator coordinator) {
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

        byte[] entry = exchange.getRequestBody().readNBytes(BODY_SIZE_LIMIT + 1);
        if (entry.length > BODY_SIZE_LIMIT) {
            exchange.sendResponseHeaders(413, -1);
            exchange.close();
            return;
        }

        coordinator.upsert(entryId, entry, ack, internal);

        exchange.getResponseHeaders().set("Content-Type", "text/html; charset=utf-8");
        exchange.sendResponseHeaders(201, -1);
        exchange.close();
    }
}
