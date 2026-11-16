package company.vk.edu.distrib.compute.akravchenya.kv;

import com.sun.net.httpserver.HttpExchange;
import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.net.URLDecoder;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Обрабатывает CRUD API на {@code /v0/entity}. Внутренние запросы
 * между узлами кластера выполняются локально, остальные координирует
 * {@link ReplicaCoordinator} с учетом параметра {@code ack}.
 */
final class EntityHandler implements KVRequestHandler.ThrowingHandler {

    private static final String ID_PARAMETER = "id";
    private static final String ACK_PARAMETER = "ack";

    private final ReplicaCoordinator coordinator;

    EntityHandler(Dao<byte[]> dao, int selfPort, HttpClient client) {
        this.coordinator = new ReplicaCoordinator(dao, selfPort, client);
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        var params = parseQuery(exchange);
        var id = requireId(params);
        var internal = exchange.getRequestHeaders().containsKey(ReplicaCoordinator.INTERNAL_REQUEST_HEADER);
        var ack = internal ? 1 : coordinator.ackOf(params.get(ACK_PARAMETER));
        switch (exchange.getRequestMethod()) {
            case "GET" -> respondGet(exchange, id, ack, internal);
            case "PUT" -> respondPut(exchange, id, ack, internal);
            case "DELETE" -> respondDelete(exchange, id, ack, internal);
            default -> KVRequestHandler.sendEmpty(exchange, 405);
        }
    }

    private void respondGet(HttpExchange exchange, String id, int ack, boolean internal) throws IOException {
        var value = internal ? coordinator.getLocal(id) : coordinator.get(id, ack);
        exchange.sendResponseHeaders(200, value.length);
        try (var stream = exchange.getResponseBody()) {
            stream.write(value);
        }
    }

    private void respondPut(HttpExchange exchange, String id, int ack, boolean internal) throws IOException {
        var value = exchange.getRequestBody().readAllBytes();
        if (internal) {
            coordinator.upsertLocal(id, value);
        } else {
            coordinator.upsert(id, value, ack);
        }
        KVRequestHandler.sendEmpty(exchange, 201);
    }

    private void respondDelete(HttpExchange exchange, String id, int ack, boolean internal) throws IOException {
        if (internal) {
            coordinator.deleteLocal(id);
            KVRequestHandler.sendEmpty(exchange, 202);
        } else {
            coordinator.delete(id, ack);
            KVRequestHandler.sendEmpty(exchange, coordinator.replicationEnabled() ? 200 : 202);
        }
    }

    private static Map<String, String> parseQuery(HttpExchange exchange) {
        var params = new LinkedHashMap<String, String>();
        var query = exchange.getRequestURI().getRawQuery();
        if (query == null) {
            return params;
        }
        for (var parameter : query.split("&")) {
            var separator = parameter.indexOf('=');
            var name = separator < 0 ? parameter : parameter.substring(0, separator);
            var value = separator < 0 ? "" : parameter.substring(separator + 1);
            params.putIfAbsent(
                URLDecoder.decode(name, StandardCharsets.UTF_8),
                URLDecoder.decode(value, StandardCharsets.UTF_8));
        }
        return params;
    }

    /**
     * Извлечь и проверить параметр {@code id} из query строки запроса.
     *
     * @param params параметры запроса
     * @return непустой идентификатор
     * @throws IllegalArgumentException если параметр отсутствует или пуст
     */
    private static String requireId(Map<String, String> params) {
        var id = params.get(ID_PARAMETER);
        if (id == null || id.isEmpty()) {
            throw new IllegalArgumentException("missing or empty query parameter " + ID_PARAMETER);
        }
        return id;
    }
}
