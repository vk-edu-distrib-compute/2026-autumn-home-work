package company.vk.edu.distrib.compute.playingpeano.urlshortener;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.urlshortener.UrlShortenerService;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.SecureRandom;
import java.util.NoSuchElementException;

import static company.vk.edu.distrib.compute.playingpeano.urlshortener.HttpUtils.hasTextContentType;
import static company.vk.edu.distrib.compute.playingpeano.urlshortener.HttpUtils.readBody;
import static company.vk.edu.distrib.compute.playingpeano.urlshortener.HttpUtils.sendEmpty;
import static company.vk.edu.distrib.compute.playingpeano.urlshortener.HttpUtils.sendMethodNotAllowed;

final class UrlShortenerServiceImpl implements UrlShortenerService {
    private static final String LINKS_PATH = "/v0/links";
    private static final String LINK_PATH_PREFIX = LINKS_PATH + "/";
    private static final String USERS_PATH = "/internal/users";
    private static final String STATUS_PATH = "/v0/status";
    private static final String GET_METHOD = "GET";
    private static final String POST_METHOD = "POST";
    private static final String PUT_METHOD = "PUT";
    private static final String DELETE_METHOD = "DELETE";

    private final int port;
    private final PersistentStringDao links;
    private final BasicAuthenticator authentication;
    private final HttpServer server;
    private final SecureRandom random = new SecureRandom();

    UrlShortenerServiceImpl(int port, PersistentStringDao links, PersistentStringDao users) throws IOException {
        this.port = port;
        this.links = links;
        authentication = new BasicAuthenticator(users);
        server = HttpServer.create(new InetSocketAddress("localhost", port), 0);
        server.createContext("/", this::handle);
    }

    @Override
    public void start() {
        server.start();
    }

    @Override
    public void stop() {
        try (links; authentication) {
            server.stop(0);
        }
    }

    private void handle(HttpExchange exchange) throws IOException {
        try (exchange) {
            try {
                route(exchange);
            } catch (NoSuchElementException exception) {
                sendEmpty(exchange, 404);
            } catch (IllegalArgumentException exception) {
                sendEmpty(exchange, 422);
            } catch (IOException exception) {
                sendEmpty(exchange, 503);
            } catch (RuntimeException exception) {
                sendEmpty(exchange, 500);
            }
        }
    }

    private void route(HttpExchange exchange) throws IOException {
        String path = exchange.getRequestURI().getPath();
        String method = exchange.getRequestMethod();

        if (STATUS_PATH.equals(path)) {
            handleStatus(exchange, method);
        } else if (USERS_PATH.equals(path)) {
            handleUsers(exchange, method);
        } else if (LINKS_PATH.equals(path)) {
            if (isAuthenticated(exchange)) {
                handleLinkCollection(exchange, method);
            }
        } else if (path.startsWith(LINK_PATH_PREFIX)) {
            if (isAuthenticated(exchange)) {
                handleLink(exchange, method, path.substring(LINK_PATH_PREFIX.length()));
            }
        } else {
            handleRedirect(exchange, method, path);
        }
    }

    private void handleStatus(HttpExchange exchange, String method) throws IOException {
        if (!GET_METHOD.equals(method)) {
            sendMethodNotAllowed(exchange, GET_METHOD);
            return;
        }
        sendEmpty(exchange, links.isAvailable() && authentication.isAvailable() ? 200 : 503);
    }

    private void handleUsers(HttpExchange exchange, String method) throws IOException {
        if (!POST_METHOD.equals(method)) {
            sendMethodNotAllowed(exchange, POST_METHOD);
            return;
        }
        if (!hasTextContentType(exchange)) {
            sendEmpty(exchange, 415);
            return;
        }

        authentication.register(readBody(exchange));
        sendEmpty(exchange, 200);
    }

    private void handleLinkCollection(HttpExchange exchange, String method) throws IOException {
        if (!POST_METHOD.equals(method)) {
            sendMethodNotAllowed(exchange, POST_METHOD);
            return;
        }
        if (!hasTextContentType(exchange)) {
            sendEmpty(exchange, 415);
            return;
        }

        String longLink = readBody(exchange);
        String id = createLink(longLink);
        HttpUtils.sendText(exchange, 201, "http://localhost:" + port + "/" + id);
    }

    private void handleLink(HttpExchange exchange, String method, String id) throws IOException {
        HttpUtils.validateId(id);
        switch (method) {
            case GET_METHOD -> HttpUtils.sendText(exchange, 200, links.get(id));
            case PUT_METHOD -> updateLink(exchange, id);
            case DELETE_METHOD -> deleteLink(exchange, id);
            default -> sendMethodNotAllowed(exchange, "GET, PUT, DELETE");
        }
    }

    private void updateLink(HttpExchange exchange, String id) throws IOException {
        if (!hasTextContentType(exchange)) {
            sendEmpty(exchange, 415);
            return;
        }
        String longLink = readBody(exchange);
        HttpUtils.validateLongLink(longLink);
        links.get(id);
        links.upsert(id, longLink);
        sendEmpty(exchange, 200);
    }

    private void deleteLink(HttpExchange exchange, String id) throws IOException {
        links.delete(id);
        sendEmpty(exchange, 202);
    }

    private void handleRedirect(HttpExchange exchange, String method, String path) throws IOException {
        if (!GET_METHOD.equals(method)) {
            sendMethodNotAllowed(exchange, GET_METHOD);
            return;
        }
        if (path.length() <= 1 || path.charAt(0) != '/') {
            sendEmpty(exchange, 404);
            return;
        }
        String id = path.substring(1);
        HttpUtils.validateId(id);
        exchange.getResponseHeaders().set("Location", links.get(id));
        sendEmpty(exchange, 301);
    }

    private boolean isAuthenticated(HttpExchange exchange) throws IOException {
        if (authentication.authenticate(exchange.getRequestHeaders().getFirst("Authorization"))) {
            return true;
        }
        HttpUtils.sendUnauthorized(exchange);
        return false;
    }

    private String createLink(String longLink) throws IOException {
        HttpUtils.validateLongLink(longLink);
        String id;
        do {
            id = HttpUtils.randomId(random);
        } while (linkExists(id));
        links.upsert(id, longLink);
        return id;
    }

    private boolean linkExists(String id) throws IOException {
        try {
            links.get(id);
            return true;
        } catch (NoSuchElementException exception) {
            return false;
        }
    }

}
