package company.vk.edu.distrib.compute.akravchenya.kv;

import com.sun.net.httpserver.HttpExchange;

import java.io.IOException;

/**
 * Проверка доступности на {@code GET /v0/status}.
 */
final class StatusHandler implements KVRequestHandler.ThrowingHandler {

    private static final String GET = "GET";

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        if (!GET.equals(exchange.getRequestMethod())) {
            KVRequestHandler.sendEmpty(exchange, 405);
            return;
        }
        KVRequestHandler.sendEmpty(exchange, 200);
    }
}
