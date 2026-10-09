package company.vk.edu.distrib.compute.nickmish.kv;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.NoSuchElementException;
import java.util.function.BooleanSupplier;

public final class KVHandler implements HttpHandler {
    private static final String GET = "GET";
    private static final String PUT = "PUT";
    private static final String DELETE = "DELETE";
    private static final String STATUS_PATH = "/v0/status";
    private static final String ENTITY_PATH = "/v0/entity";
    private static final String CONTENT_TYPE = "application/octet-stream";

    private final Dao<byte[]> dao;
    private final BooleanSupplier available;

    public KVHandler(Dao<byte[]> dao, BooleanSupplier available) {
        this.dao = dao;
        this.available = available;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        try (exchange) {
            Response response = process(exchange);
            exchange.getResponseHeaders().set("Content-Type", CONTENT_TYPE);
            if (response.body().length == 0) {
                exchange.sendResponseHeaders(response.status(), -1);
            } else {
                exchange.sendResponseHeaders(response.status(), response.body().length);
                exchange.getResponseBody().write(response.body());
            }
        }
    }

    private Response process(HttpExchange exchange) {
        try {
            return route(exchange);
        } catch (NoSuchElementException e) {
            return new Response(404, new byte[0]);
        } catch (IllegalArgumentException e) {
            return new Response(400, new byte[0]);
        } catch (IOException e) {
            return new Response(503, new byte[0]);
        }
    }

    private Response route(HttpExchange exchange) throws IOException {
        String path = exchange.getRequestURI().getRawPath();
        String method = exchange.getRequestMethod();

        if (STATUS_PATH.equals(path)) {
            return status(exchange, method);
        }

        if (ENTITY_PATH.equals(path)) {
            return entity(exchange, method);
        }

        return new Response(404, new byte[0]);
    }

    private Response status(HttpExchange exchange, String method) {
        if (!GET.equals(method)) {
            return methodNotAllowed(exchange, "GET");
        }
        return available.getAsBoolean() ? new Response(200, new byte[0]) : new Response(503, new byte[0]);
    }

    private Response entity(HttpExchange exchange, String method) throws IOException {
        String query = exchange.getRequestURI().getRawQuery();
        if (query == null || !query.startsWith("id=")) {
            return new Response(400, new byte[0]);
        }
        String id = URLDecoder.decode(query.substring(3), StandardCharsets.UTF_8);
        if (id.isEmpty()) {
            return new Response(400, new byte[0]);
        }

        return switch (method) {
            case GET -> get(id);
            case PUT -> {
                byte[] body = readBody(exchange);
                dao.upsert(id, body);
                yield new Response(201, new byte[0]);
            }
            case DELETE -> {
                dao.delete(id);
                yield new Response(202, new byte[0]);
            }
            default -> methodNotAllowed(exchange, "GET, PUT, DELETE");
        };
    }

    private Response get(String id) throws IOException {
        try {
            byte[] value = dao.get(id);
            return new Response(200, value);
        } catch (NoSuchElementException e) {
            return new Response(404, new byte[0]);
        }
    }

    private static Response methodNotAllowed(HttpExchange exchange, String methods) {
        exchange.getResponseHeaders().set("Allow", methods);
        return new Response(405, new byte[0]);
    }

    private static byte[] readBody(HttpExchange exchange) throws IOException {
        return exchange.getRequestBody().readAllBytes();
    }

    private record Response(int status, byte[] body) {
    }
}
