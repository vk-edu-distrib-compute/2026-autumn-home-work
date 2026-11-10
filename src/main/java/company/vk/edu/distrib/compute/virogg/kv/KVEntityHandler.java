package company.vk.edu.distrib.compute.virogg.kv;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.virogg.http.HttpUtils;
import org.jspecify.annotations.Nullable;

final class KVEntityHandler implements HttpHandler {
    private final Dao<byte[]> dao;
    @Nullable
    private final ReplicationCoordinator coordinator;

    KVEntityHandler(Dao<byte[]> dao) {
        this.dao = dao;
        coordinator = null;
    }

    KVEntityHandler(Dao<byte[]> dao, ReplicationCoordinator coordinator) {
        this.dao = dao;
        this.coordinator = coordinator;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        if (!KvApiConstants.ENTITY_PATH.equals(exchange.getRequestURI().getPath())) {
            HttpUtils.sendEmpty(exchange, HttpURLConnection.HTTP_NOT_FOUND);
            return;
        }
        String method = exchange.getRequestMethod();
        if (!HttpUtils.GET.equals(method) && !HttpUtils.PUT.equals(method) && !HttpUtils.DELETE.equals(method)) {
            HttpUtils.sendEmpty(exchange, HttpURLConnection.HTTP_BAD_METHOD);
            return;
        }
        execute(exchange, method);
    }

    private void execute(HttpExchange exchange, String method) throws IOException {
        ReplicationConfig config = coordinator == null ? null : coordinator.configuration();
        KvEntityQuery query;
        try {
            query = KvEntityQuery.parse(exchange.getRequestURI().getRawQuery(),
                    config == null ? 1 : config.replicationFactor());
        } catch (IllegalArgumentException e) {
            HttpUtils.sendEmpty(exchange, HttpURLConnection.HTTP_BAD_REQUEST);
            return;
        }
        respond(exchange, dispatch(exchange, method, query, config));
    }

    private ReplicaResponse dispatch(HttpExchange exchange, String method, KvEntityQuery query,
                                     @Nullable ReplicationConfig config) throws IOException {
        byte[] body = HttpUtils.PUT.equals(method) ? exchange.getRequestBody().readAllBytes() : new byte[0];
        boolean internal = "true".equals(exchange.getRequestHeaders().getFirst(KvApiConstants.REPLICA_HEADER));
        return coordinator == null || config == null || config.isStandalone() || internal
                ? ReplicationCoordinator.local(dao, method, query.key(), body)
                : coordinator.execute(method, query.key(), body, query.ack(), config);
    }

    private static void respond(HttpExchange exchange, ReplicaResponse response) throws IOException {
        if (response.status() != HttpURLConnection.HTTP_OK) {
            HttpUtils.sendEmpty(exchange, response.status());
            return;
        }
        byte[] value = response.body();
        exchange.getResponseHeaders().set("Content-Type", "application/octet-stream");
        exchange.sendResponseHeaders(response.status(), value.length == 0 ? -1 : value.length);
        try (OutputStream output = exchange.getResponseBody()) {
            output.write(value);
        }
    }
}
