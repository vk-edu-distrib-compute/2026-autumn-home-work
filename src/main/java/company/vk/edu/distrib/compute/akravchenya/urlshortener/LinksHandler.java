package company.vk.edu.distrib.compute.akravchenya.urlshortener;

import com.sun.net.httpserver.HttpExchange;
import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Обрабатывает CRUD API на {@code /v0/links}.
 */
final class LinksHandler implements RequestHandler.ThrowingHandler {

    private static final String ID_ALPHABET =
        "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    private static final int ID_LENGTH = 10;

    private final Dao<String> links;
    private final Authentication authentication;
    private final int port;

    LinksHandler(Dao<String> links, Authentication authentication, int port) {
        this.links = links;
        this.authentication = authentication;
        this.port = port;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        authentication.require(exchange);
        var id = Urls.idAfterContext(exchange);
        switch (exchange.getRequestMethod()) {
            case "GET" -> respondGet(exchange, id);
            case "POST" -> respondCreate(exchange, id);
            case "PUT" -> respondUpdate(exchange, id);
            case "DELETE" -> respondDelete(exchange, id);
            default -> RequestHandler.sendEmpty(exchange, 405);
        }
    }

    private void respondGet(HttpExchange exchange, String id) throws IOException {
        if (id.isEmpty()) {
            RequestHandler.sendEmpty(exchange, 405);
            return;
        }
        RequestHandler.send(exchange, 200, links.get(Urls.requireId(id)));
    }

    private void respondCreate(HttpExchange exchange, String id) throws IOException {
        if (!id.isEmpty()) {
            throw new IllegalArgumentException("link id is not allowed in a create request");
        }
        var longLink = Urls.readBody(exchange);
        Urls.requireLongLink(longLink);
        var linkId = generateFreeId();
        links.upsert(linkId, longLink);
        RequestHandler.send(exchange, 201, shortLink(linkId));
    }

    private void respondUpdate(HttpExchange exchange, String id) throws IOException {
        if (id.isEmpty()) {
            RequestHandler.sendEmpty(exchange, 405);
            return;
        }
        var linkId = Urls.requireId(id);
        var longLink = Urls.readBody(exchange);
        Urls.requireLongLink(longLink);
        links.get(linkId);
        links.upsert(linkId, longLink);
        RequestHandler.sendEmpty(exchange, 200);
    }

    private void respondDelete(HttpExchange exchange, String id) throws IOException {
        if (id.isEmpty()) {
            RequestHandler.sendEmpty(exchange, 405);
            return;
        }
        links.delete(Urls.requireId(id));
        RequestHandler.sendEmpty(exchange, 202);
    }

    private String generateFreeId() throws IOException {
        var id = new StringBuilder(ID_LENGTH);
        while (true) {
            id.setLength(0);
            for (var i = 0; i < ID_LENGTH; i++) {
                id.append(ID_ALPHABET.charAt(ThreadLocalRandom.current().nextInt(ID_ALPHABET.length())));
            }
            var candidate = id.toString();
            try {
                links.get(candidate);
            } catch (NoSuchElementException exception) {
                return candidate;
            }
        }
    }

    private String shortLink(String id) {
        return "http://localhost:" + port + "/" + id;
    }
}
