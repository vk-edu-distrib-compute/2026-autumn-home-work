package company.vk.edu.distrib.compute.nosorozhek.kv.handlers;

import com.sun.net.httpserver.HttpExchange;
import company.vk.edu.distrib.compute.nosorozhek.kv.ReplicationCoordinator;

import java.io.IOException;

public class DeleteEntryHandler implements Handler {
    private final ReplicationCoordinator coordinator;

    public DeleteEntryHandler(ReplicationCoordinator coordinator) {
        this.coordinator = coordinator;
    }

    @Override
    public void handle(HttpExchange exchange, RouteParameters parameters) throws IOException {
        String entryId = RouteParameters.requiredId(exchange);
        boolean internal = ReplicationCoordinator.isInternalRequest(exchange);
        int ack = internal ? 1 : RouteParameters.optionalAck(exchange).orElse(1);

        coordinator.delete(entryId, ack, internal);

        exchange.sendResponseHeaders(internal || !coordinator.isReplicated() ? 202 : 200, -1);
        exchange.close();
    }
}
