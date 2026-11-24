package company.vk.edu.distrib.compute.akravchenya.urlshortener;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.NoSuchElementException;

/**
 * Обработчик запросов.
 */
final class RequestHandler implements HttpHandler {

    private static final String CONTENT_TYPE = "text/html; charset=utf-8";

    private final ThrowingHandler delegate;

    RequestHandler(ThrowingHandler delegate) {
        this.delegate = delegate;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        try {
            delegate.handle(exchange);
        } catch (NoSuchElementException exception) {
            send(exchange, 404, exception.getMessage());
        } catch (IllegalArgumentException exception) {
            send(exchange, 422, exception.getMessage());
        } catch (UnauthorizedException exception) {
            send(exchange, 401, exception.getMessage());
        } catch (Exception exception) {
            send(exchange, 500, "Internal Server Error");
        }
    }

    /**
     * Отправить HTTP ответ с заданным статусом и телом.
     *
     * @param exchange обмен, на который отвечаем
     * @param code     HTTP статус
     * @param body     тело ответа
     * @throws IOException если не удалось отправить ответ
     */
    static void send(HttpExchange exchange, int code, String body) throws IOException {
        var bytes = body == null ? new byte[0] : body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", CONTENT_TYPE);
        exchange.sendResponseHeaders(code, bytes.length);
        try (var stream = exchange.getResponseBody()) {
            stream.write(bytes);
        }
    }

    /**
     * Отправить HTTP ответ с заданным статусом без тела.
     *
     * @param exchange обмен, на который отвечаем
     * @param code     HTTP статус
     * @throws IOException если не удалось отправить ответ
     */
    static void sendEmpty(HttpExchange exchange, int code) throws IOException {
        exchange.sendResponseHeaders(code, -1);
    }

    @FunctionalInterface
    interface ThrowingHandler {
        void handle(HttpExchange exchange) throws IOException;
    }
}
