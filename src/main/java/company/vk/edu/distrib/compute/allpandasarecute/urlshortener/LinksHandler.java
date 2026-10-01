package company.vk.edu.distrib.compute.allpandasarecute.urlshortener;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.NoSuchElementException;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import company.vk.edu.distrib.compute.Dao;

class LinksHandler implements HttpHandler {
    private static final Logger log = LoggerFactory.getLogger(LinksHandler.class);

    private static final String GET_METHOD = "GET";
    private static final String POST_METHOD = "POST";
    private static final String PUT_METHOD = "PUT";
    private static final String DELETE_METHOD = "DELETE";
    private static final String COLLECTION_PATH = "/v0/links";
    private static final String ITEM_PREFIX = "/v0/links/";
    private static final String AUTHENTICATE_CHALLENGE = "Basic realm=\"url-shortener\"";

    private final int port;
    private final Dao<String> links;
    private final BasicAuthenticator authenticator;

    LinksHandler(int port, Dao<String> links, BasicAuthenticator authenticator) {
        this.port = port;
        this.links = links;
        this.authenticator = authenticator;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        try (exchange) {
            try {
                dispatch(exchange);
            } catch (NoSuchElementException expected) {
                HttpResponses.sendEmpty(exchange, 404);
            } catch (IOException | RuntimeException e) {
                log.error("Failed to handle links request", e);
                HttpResponses.sendEmpty(exchange, 500);
            }
        }
    }

    private void dispatch(HttpExchange exchange) throws IOException {
        if (!authenticator.authenticate(exchange)) {
            exchange.getResponseHeaders().set("WWW-Authenticate", AUTHENTICATE_CHALLENGE);
            HttpResponses.sendEmpty(exchange, 401);
            return;
        }
        String path = exchange.getRequestURI().getPath();
        if (COLLECTION_PATH.equals(path)) {
            create(exchange);
            return;
        }
        String id = path.substring(ITEM_PREFIX.length());
        switch (exchange.getRequestMethod()) {
            case GET_METHOD -> send(exchange, id);
            case PUT_METHOD -> replace(exchange, id);
            case DELETE_METHOD -> remove(exchange, id);
            default -> HttpResponses.sendEmpty(exchange, 405);
        }
    }

    private void create(HttpExchange exchange) throws IOException {
        if (!POST_METHOD.equals(exchange.getRequestMethod())) {
            HttpResponses.sendEmpty(exchange, 405);
            return;
        }
        String longLink = HttpResponses.readBody(exchange);
        if (!isValidLink(longLink)) {
            HttpResponses.sendEmpty(exchange, 422);
            return;
        }
        String id = Ids.generate();
        links.upsert(id, longLink);
        HttpResponses.sendBody(exchange, 201, "http://localhost:%d/%s".formatted(port, id));
    }

    private void send(HttpExchange exchange, String id) throws IOException {
        if (!Ids.isValid(id)) {
            HttpResponses.sendEmpty(exchange, 422);
            return;
        }
        HttpResponses.sendBody(exchange, 200, links.get(id));
    }

    private void replace(HttpExchange exchange, String id) throws IOException {
        if (!Ids.isValid(id)) {
            HttpResponses.sendEmpty(exchange, 422);
            return;
        }
        String longLink = HttpResponses.readBody(exchange);
        if (!isValidLink(longLink)) {
            HttpResponses.sendEmpty(exchange, 422);
            return;
        }
        links.get(id);
        links.upsert(id, longLink);
        HttpResponses.sendEmpty(exchange, 200);
    }

    private void remove(HttpExchange exchange, String id) throws IOException {
        if (!Ids.isValid(id)) {
            HttpResponses.sendEmpty(exchange, 422);
            return;
        }
        links.delete(id);
        HttpResponses.sendEmpty(exchange, 202);
    }

    private static boolean isValidLink(String link) {
        try {
            URI uri = new URI(link);
            return ("http".equals(uri.getScheme()) || "https".equals(uri.getScheme()))
                && uri.getHost() != null;
        } catch (URISyntaxException e) {
            return false;
        }
    }
}
