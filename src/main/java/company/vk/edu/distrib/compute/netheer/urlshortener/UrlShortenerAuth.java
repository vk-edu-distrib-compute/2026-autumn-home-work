package company.vk.edu.distrib.compute.netheer.urlshortener;

import com.sun.net.httpserver.HttpExchange;
import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.NoSuchElementException;

public class UrlShortenerAuth {
    private static final String USERS_PATH = "/internal/users";
    private static final String POST_METHOD = "POST";
    private final Dao<String> usersDao;

    UrlShortenerAuth(Dao<String> usersDao) {
        this.usersDao = usersDao;
    }

    private String readBody(HttpExchange exchange) throws IOException {
        return new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
    }

    void handleUsers(HttpExchange exchange) throws IOException {
        try (exchange) {
            if (!USERS_PATH.equals(exchange.getRequestURI().getPath())) {
                exchange.sendResponseHeaders(404, -1);
                return;
            }

            if (!POST_METHOD.equals(exchange.getRequestMethod())) {
                sendMethodNotAllowed(exchange, "POST");
                return;
            }

            String credentials = readBody(exchange);
            int separator = credentials.indexOf(':');

            if (separator < 0 || credentials.chars().anyMatch(character -> character < 32 || character == 127)) {
                exchange.sendResponseHeaders(422, -1);
                return;
            }

            String username = credentials.substring(0, separator);
            String password = credentials.substring(separator + 1);

            usersDao.upsert(username, password);
            exchange.sendResponseHeaders(200, -1);
        }
    }

    boolean authenticate(HttpExchange exchange) throws IOException {
        String authorization = exchange.getRequestHeaders().getFirst("Authorization");

        if (authorization != null && checkCredentials(authorization)) {
            return true;
        }

        exchange.getResponseHeaders().set(
                "WWW-Authenticate",
                "Basic realm=\"urlshortener\", charset=\"UTF-8\""
        );
        exchange.sendResponseHeaders(401, -1);
        return false;
    }

    private boolean checkCredentials(String authorization) throws IOException {
        String[] parts = authorization.split(" +", 2);
        if (parts.length != 2 || !"Basic".equalsIgnoreCase(parts[0])) {
            return false;
        }

        String credentials;
        try {
            byte[] decoded = Base64.getDecoder().decode(parts[1]);
            credentials = new String(decoded, StandardCharsets.UTF_8);
        } catch (IllegalArgumentException exception) {
            return false;
        }

        int separator = credentials.indexOf(':');
        if (separator < 0 || credentials.chars().anyMatch(character -> character < 32 || character == 127)) {
            return false;
        }

        String username = credentials.substring(0, separator);
        String password = credentials.substring(separator + 1);

        try {
            return password.equals(usersDao.get(username));
        } catch (NoSuchElementException exception) {
            return false;
        }
    }

    private void sendMethodNotAllowed(HttpExchange exchange, String allowedMethods) throws IOException {
        exchange.getResponseHeaders().set("Allow", allowedMethods);
        exchange.sendResponseHeaders(405, -1);
    }
}
