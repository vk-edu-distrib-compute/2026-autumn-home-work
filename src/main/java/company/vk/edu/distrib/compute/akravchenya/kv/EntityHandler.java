package company.vk.edu.distrib.compute.akravchenya.kv;

import com.sun.net.httpserver.HttpExchange;
import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Обрабатывает CRUD API на {@code /v0/entity}.
 */
final class EntityHandler implements KVRequestHandler.ThrowingHandler {

    private static final String ENTITY_PARAMETER = "id=";

    private final Dao<byte[]> dao;

    EntityHandler(Dao<byte[]> dao) {
        this.dao = dao;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        var id = requireId(exchange);
        switch (exchange.getRequestMethod()) {
            case "GET" -> respondGet(exchange, id);
            case "PUT" -> respondPut(exchange, id);
            case "DELETE" -> respondDelete(exchange, id);
            default -> KVRequestHandler.sendEmpty(exchange, 405);
        }
    }

    private void respondGet(HttpExchange exchange, String id) throws IOException {
        var value = dao.get(id);
        exchange.sendResponseHeaders(200, value.length);
        try (var stream = exchange.getResponseBody()) {
            stream.write(value);
        }
    }

    private void respondPut(HttpExchange exchange, String id) throws IOException {
        var value = exchange.getRequestBody().readAllBytes();
        dao.upsert(id, value);
        KVRequestHandler.sendEmpty(exchange, 201);
    }

    private void respondDelete(HttpExchange exchange, String id) throws IOException {
        dao.delete(id);
        KVRequestHandler.sendEmpty(exchange, 202);
    }

    /**
     * Извлечь и проверить параметр {@code id} из query строки запроса.
     *
     * @param exchange проверяемый обмен данными
     * @throws IllegalArgumentException если параметр отсутствует или пуст
     */
    private static String requireId(HttpExchange exchange) {
        var query = exchange.getRequestURI().getRawQuery();
        if (query == null) {
            throw new IllegalArgumentException("missing query parameter " + ENTITY_PARAMETER);
        }
        return Arrays.stream(query.split("&"))
            .filter(parameter -> parameter.startsWith(ENTITY_PARAMETER))
            .map(parameter -> parameter.substring(ENTITY_PARAMETER.length()))
            .map(parameter -> URLDecoder.decode(parameter, StandardCharsets.UTF_8))
            .filter(parameter -> !parameter.isEmpty())
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException("missing or empty query parameter " + ENTITY_PARAMETER));
    }
}
