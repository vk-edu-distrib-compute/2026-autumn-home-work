package company.vk.edu.distrib.compute.flighen.urlshortener;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

public class GetStatusHandler implements HttpHandler {

    public static final String UP = "UP";

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        final var method = exchange.getRequestMethod();

        if (Objects.equals(method, "GET")) {
            exchange.sendResponseHeaders(200, UP.length());
            exchange.getResponseBody().write(UP.getBytes(StandardCharsets.UTF_8));
        } else {
            exchange.sendResponseHeaders(503, 0);
        }

        exchange.close();
    }

}
