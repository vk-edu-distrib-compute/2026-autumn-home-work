package company.vk.edu.distrib.compute.fedorkhokhryakov.urlshortener;

import com.sun.net.httpserver.HttpExchange;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.NoSuchElementException;

public class UrlShortenerHttpHandler {
private static final String CONTENT_TYPE = "text/html; charset=utf-8";
private static final String GET = "GET";
private static final String POST = "POST";
private static final String PUT = "PUT";
private static final String DELETE = "DELETE";
private static final String LINKS_PATH = "/v0/links";
private static final String ROOT_PATH = "/";
private static final String LOCATION = "Location";
private static final String WWW_AUTHENTICATE = "WWW-Authenticate";
private static final String BASIC = "Basic";

private final int port;
private final InMemoryDao<String> dao;
private final InMemoryUserDao userDao;
private final BasicAuthenticator authenticator;

public UrlShortenerHttpHandler(
    int port,
    InMemoryDao<String> dao,
    InMemoryUserDao userDao
) {
    this.port = port;
    this.dao = dao;
    this.userDao = userDao;
    this.authenticator = new BasicAuthenticator(userDao);
}

public void handleStatus(HttpExchange exchange) throws IOException {
    if (!GET.equals(exchange.getRequestMethod())) {
        sendResponse(exchange, 404, "");
        return;
    }

    sendResponse(exchange, 200, "");
}

public void handleLinks(HttpExchange exchange) throws IOException {
    if (!authenticator.authenticate(exchange)) {
        sendUnauthorized(exchange);
        return;
    }

    String path = exchange.getRequestURI().getPath();

    if (isLinksRoot(path)) {
        handleLinksRoot(exchange);
        return;
    }

    if (!path.startsWith(LINKS_PATH + ROOT_PATH)) {
        sendResponse(exchange, 404, "");
        return;
    }

    String id = path.substring((LINKS_PATH + ROOT_PATH).length());

    if (!UrlShortenerUtils.isValidId(id)) {
        sendResponse(exchange, 422, "");
        return;
    }

    handleLinkById(exchange, id);
}

public void handleRedirect(HttpExchange exchange) throws IOException {
    if (!GET.equals(exchange.getRequestMethod())) {
        sendResponse(exchange, 404, "");
        return;
    }

    String path = exchange.getRequestURI().getPath();

    if (path.length() <= 1 || !path.startsWith(ROOT_PATH)) {
        sendResponse(exchange, 404, "");
        return;
    }

    String id = path.substring(ROOT_PATH.length());

    if (!UrlShortenerUtils.isValidId(id)) {
        sendResponse(exchange, 422, "");
        return;
    }

    try (exchange) {
        try {
            String longLink = dao.get(id);

            exchange.getResponseHeaders().set(LOCATION, longLink);
            exchange.sendResponseHeaders(301, -1);
        } catch (NoSuchElementException e) {
            sendResponse(exchange, 404, "");
        }
    }
}

public void handleUsers(HttpExchange exchange) throws IOException {
    if (!POST.equals(exchange.getRequestMethod())) {
        sendResponse(exchange, 404, "");
        return;
    }

    String body = readBody(exchange);
    int separator = body.indexOf(':');

    if (separator <= 0 || separator == body.length() - 1) {
        sendResponse(exchange, 422, "");
        return;
    }

    String username = body.substring(0, separator);
    String password = body.substring(separator + 1);

    userDao.upsert(username, password);
    sendResponse(exchange, 200, "");
}

private void handleLinksRoot(HttpExchange exchange) throws IOException {
    if (POST.equals(exchange.getRequestMethod())) {
        handleCreate(exchange);
    } else {
        sendResponse(exchange, 404, "");
    }
}

private void handleLinkById(HttpExchange exchange, String id) throws IOException {
    switch (exchange.getRequestMethod()) {
        case GET -> handleGet(exchange, id);
        case PUT -> handleUpdate(exchange, id);
        case DELETE -> handleDelete(exchange, id);
        default -> sendResponse(exchange, 404, "");
    }
}

private void handleCreate(HttpExchange exchange) throws IOException {
    String longLink = readBody(exchange);

    if (!UrlShortenerUtils.isValidLink(longLink)) {
        sendResponse(exchange, 422, "");
        return;
    }

    String id;

    do {
        id = UrlShortenerUtils.generateId();
    } while (exists(id));

    dao.upsert(id, longLink);

    String shortLink = "http://localhost:" + port + "/" + id;

    sendResponse(exchange, 201, shortLink);
}

private void handleGet(HttpExchange exchange, String id) throws IOException {
    try {
        String longLink = dao.get(id);
        sendResponse(exchange, 200, longLink);
    } catch (NoSuchElementException e) {
        sendResponse(exchange, 404, "");
    }
}

private void handleUpdate(HttpExchange exchange, String id) throws IOException {
    String longLink = readBody(exchange);

    if (!UrlShortenerUtils.isValidLink(longLink)) {
        sendResponse(exchange, 422, "");
        return;
    }

    try {
        dao.get(id);
    } catch (NoSuchElementException e) {
        sendResponse(exchange, 404, "");
        return;
    }

    dao.upsert(id, longLink);
    sendResponse(exchange, 200, "");
}

private void handleDelete(HttpExchange exchange, String id) throws IOException {
    dao.delete(id);
    sendResponse(exchange, 202, "");
}

private String readBody(HttpExchange exchange) throws IOException {
    return new String(
        exchange.getRequestBody().readAllBytes(),
        StandardCharsets.UTF_8
    );
}

private boolean exists(String id) {
    try {
        dao.get(id);
        return true;
    } catch (NoSuchElementException e) {
        return false;
    } catch (IOException e) {
        throw new IllegalStateException("Failed to access DAO", e);
    }
}

private static void sendResponse(HttpExchange exchange, int status, String body)
    throws IOException {
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);

    exchange.getResponseHeaders().set("Content-Type", CONTENT_TYPE);
    exchange.sendResponseHeaders(status, bytes.length);

    try (var responseBody = exchange.getResponseBody()) {
        responseBody.write(bytes);
    }
}

private void sendUnauthorized(HttpExchange exchange) throws IOException {
    exchange.getResponseHeaders().set(WWW_AUTHENTICATE, BASIC);
    sendResponse(exchange, 401, "");
}

private static boolean isLinksRoot(String path) {
    return LINKS_PATH.equals(path) || (LINKS_PATH + ROOT_PATH).equals(path);
}

}
