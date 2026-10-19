package company.vk.edu.distrib.compute.akravchenya.urlshortener;

import com.sun.net.httpserver.HttpExchange;

import java.io.IOException;

/**
 * Проверка доступности на {@code GET /v0/status}.
 */
final class StatusHandler implements RequestHandler.ThrowingHandler {

    private static final String GET = "GET";

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        if (!GET.equals(exchange.getRequestMethod())) {
            RequestHandler.sendEmpty(exchange, 405);
            return;
        }
        RequestHandler.sendEmpty(exchange, 200);
    }
}
