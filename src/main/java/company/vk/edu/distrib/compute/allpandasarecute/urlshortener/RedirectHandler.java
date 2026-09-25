package company.vk.edu.distrib.compute.allpandasarecute.urlshortener;

import java.io.IOException;
import java.util.NoSuchElementException;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import company.vk.edu.distrib.compute.Dao;

class RedirectHandler implements HttpHandler {
    private static final Logger log = LoggerFactory.getLogger(RedirectHandler.class);

    private static final String GET_METHOD = "GET";

    private final Dao<String> links;

    RedirectHandler(Dao<String> links) {
        this.links = links;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        try (exchange) {
            try {
                redirect(exchange);
            } catch (NoSuchElementException expected) {
                HttpResponses.sendEmpty(exchange, 404);
            } catch (IOException | RuntimeException e) {
                log.error("Failed to handle redirect request", e);
                HttpResponses.sendEmpty(exchange, 500);
            }
        }
    }

    private void redirect(HttpExchange exchange) throws IOException {
        if (!GET_METHOD.equals(exchange.getRequestMethod())) {
            HttpResponses.sendEmpty(exchange, 405);
            return;
        }
        String id = exchange.getRequestURI().getPath().substring(1);
        if (id.isEmpty() || id.contains("/")) {
            HttpResponses.sendEmpty(exchange, 404);
            return;
        }
        if (!Ids.isValid(id)) {
            HttpResponses.sendEmpty(exchange, 422);
            return;
        }
        exchange.getResponseHeaders().set("Location", links.get(id));
        HttpResponses.sendEmpty(exchange, 301);
    }
}
