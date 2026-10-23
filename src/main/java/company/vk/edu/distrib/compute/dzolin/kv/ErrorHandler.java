package company.vk.edu.distrib.compute.dzolin.kv;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.NoSuchElementException;
import java.util.Objects;

public class ErrorHandler implements HttpHandler {
    private final HttpHandler handler;

    public ErrorHandler(HttpHandler handler) {
        this.handler = handler;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        try (exchange) {
            try {
                handler.handle(exchange);
            } catch (NoSuchElementException exception) {
                sendError(exchange, 404, Objects.toString(exception.getMessage(), ""));
            } catch (IllegalArgumentException exception) {
                sendError(exchange, 400, Objects.toString(exception.getMessage(), ""));
            } catch (Exception exception) {
                sendError(exchange, 500, "Internal Server Error");
            }
        }
    }

    private void sendError(HttpExchange exchange, int statusCode, String message) throws IOException {
        var body = message.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "text/plain; charset=utf-8");
        exchange.sendResponseHeaders(statusCode, body.length);
        exchange.getResponseBody().write(body);
    }
}
