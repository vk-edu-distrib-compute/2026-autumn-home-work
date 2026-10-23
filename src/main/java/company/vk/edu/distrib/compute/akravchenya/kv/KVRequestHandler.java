package company.vk.edu.distrib.compute.akravchenya.kv;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * Обработчик, преобразующий исключения хранилища в HTTP статусы.
 */
final class KVRequestHandler implements HttpHandler {

    private final ThrowingHandler delegate;

    KVRequestHandler(ThrowingHandler delegate) {
        this.delegate = delegate;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        try {
            delegate.handle(exchange);
        } catch (NoSuchElementException exception) {
            sendEmpty(exchange, 404);
        } catch (IllegalArgumentException exception) {
            sendEmpty(exchange, 400);
        } catch (Exception exception) {
            sendEmpty(exchange, 500);
        }
    }

    /**
     * Отправить HTTP ответ с заданным статусом и без тела.
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
