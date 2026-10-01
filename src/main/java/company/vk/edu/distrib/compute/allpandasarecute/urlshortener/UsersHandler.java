package company.vk.edu.distrib.compute.allpandasarecute.urlshortener;

import java.io.IOException;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import company.vk.edu.distrib.compute.Dao;

class UsersHandler implements HttpHandler {
    private static final Logger log = LoggerFactory.getLogger(UsersHandler.class);

    private static final String POST_METHOD = "POST";

    private final Dao<String> users;

    UsersHandler(Dao<String> users) {
        this.users = users;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        try (exchange) {
            try {
                createUser(exchange);
            } catch (IOException | RuntimeException e) {
                log.error("Failed to handle users request", e);
                HttpResponses.sendEmpty(exchange, 500);
            }
        }
    }

    private void createUser(HttpExchange exchange) throws IOException {
        if (!POST_METHOD.equals(exchange.getRequestMethod())) {
            HttpResponses.sendEmpty(exchange, 405);
            return;
        }
        String body = HttpResponses.readBody(exchange);
        int separator = body.indexOf(':');
        if (separator <= 0 || separator == body.length() - 1) {
            HttpResponses.sendEmpty(exchange, 422);
            return;
        }
        users.upsert(body.substring(0, separator), body.substring(separator + 1));
        HttpResponses.sendEmpty(exchange, 200);
    }
}
