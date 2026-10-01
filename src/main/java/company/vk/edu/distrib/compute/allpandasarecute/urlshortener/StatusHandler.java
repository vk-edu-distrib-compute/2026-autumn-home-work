package company.vk.edu.distrib.compute.allpandasarecute.urlshortener;

import java.io.IOException;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

class StatusHandler implements HttpHandler {
    private static final String GET_METHOD = "GET";

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        try (exchange) {
            if (GET_METHOD.equals(exchange.getRequestMethod())) {
                HttpResponses.sendEmpty(exchange, 200);
            } else {
                HttpResponses.sendEmpty(exchange, 405);
            }
        }
    }
}
