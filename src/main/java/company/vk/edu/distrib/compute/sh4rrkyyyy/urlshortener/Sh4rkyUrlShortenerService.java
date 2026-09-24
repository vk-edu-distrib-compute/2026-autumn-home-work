package company.vk.edu.distrib.compute.sh4rrkyyyy.urlshortener;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.urlshortener.UrlShortenerService;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.regex.Pattern;

public class Sh4rkyUrlShortenerService implements UrlShortenerService {
    private final HttpServer server;
    private final DaoString linksDao = new DaoString("links");
    private final DaoString usersDao = new DaoString("users");
    private final int port;

    private static final String GET = "GET";
    private static final String POST = "POST";
    private static final String PUT = "PUT";
    private static final String DELETE = "DELETE";
    private static final String LINKS_PATH = "/v0/links/";
    private static final String REDIRECT_PATH = "/";
    private static final Pattern ID_PATTERN = Pattern.compile("[A-Za-z0-9]{10}");

    public Sh4rkyUrlShortenerService(int port) throws IOException {
        this.port = port;
        this.server = HttpServer.create(new InetSocketAddress(port), 0);

        server.createContext("/v0/status", new ErrorHandler(this::handleStatus));
        server.createContext("/v0/links", new ErrorHandler(new AuthHandler(this::handleLinks, usersDao)));
        server.createContext(REDIRECT_PATH, new ErrorHandler(this::handleRedirect));
        server.createContext("/internal/users", new ErrorHandler(this::handleCreateUser));

    }

    @Override
    public void start() {
        server.start();

    }

    @Override
    public void stop() {
        server.stop(1);
    }

    private void handleStatus(HttpExchange exchange) throws IOException {
        if (!GET.equals(exchange.getRequestMethod())) {
            HttpUtils.sendEmptyRsp(exchange, 405);
            return;
        }
        HttpUtils.sendEmptyRsp(exchange, 200);
    }

    private void handleLinks(HttpExchange exchange) throws IOException {
        String method = exchange.getRequestMethod();
        String path = exchange.getRequestURI().getPath();

        if ("/v0/links".equals(path)) {
            if (POST.equals(method)) {
                handleCreateLink(exchange);
            } else {
                HttpUtils.sendEmptyRsp(exchange, 405);
            }
            return;
        }

        String id = path.substring(LINKS_PATH.length());
        if (!isValidId(id)) {
            HttpUtils.sendEmptyRsp(exchange, 422);
            return;
        }

        switch (method) {
            case GET -> handleGetLink(exchange, id);
            case PUT -> handleUpdateLink(exchange, id);
            case DELETE -> handleDeleteLink(exchange, id);
            default -> HttpUtils.sendEmptyRsp(exchange, 405);
        }
    }

    private void handleRedirect(HttpExchange exchange) throws IOException {
        String id = exchange.getRequestURI().getPath().substring(REDIRECT_PATH.length());
        if (!isValidId(id)) {
            HttpUtils.sendEmptyRsp(exchange, 422);
            return;
        }
        exchange.getResponseHeaders().set("Location", linksDao.get(id));
        HttpUtils.sendEmptyRsp(exchange, 301);
    }

    private void handleCreateLink(HttpExchange exchange) throws IOException {
        String longLink = getBody(exchange);
        if (!isValidLink(longLink)) {
            HttpUtils.sendEmptyRsp(exchange, 422);
            return;
        }
        String shortId = generateShortId();
        linksDao.upsert(shortId, longLink);
        final var shortLink = "http://localhost:%d/%s".formatted(port, shortId);
        HttpUtils.sendRsp(exchange, 201, shortLink);
    }

    private void handleUpdateLink(HttpExchange exchange, String id) throws IOException {
        String newLongLink = getBody(exchange);
        if (!isValidLink(newLongLink)) {
            HttpUtils.sendEmptyRsp(exchange, 422);
            return;
        }
        linksDao.get(id);
        linksDao.upsert(id, newLongLink);
        HttpUtils.sendEmptyRsp(exchange, 200);
    }

    private void handleGetLink(HttpExchange exchange, String id) throws IOException {
        String longLink = linksDao.get(id);
        HttpUtils.sendRsp(exchange, 200, longLink);
    }

    private void handleDeleteLink(HttpExchange exchange, String id) throws IOException {
        linksDao.delete(id);
        HttpUtils.sendEmptyRsp(exchange, 202);
    }

    private void handleCreateUser(HttpExchange exchange) throws IOException {
        if (!POST.equals(exchange.getRequestMethod())) {
            HttpUtils.sendEmptyRsp(exchange, 405);
            return;
        }
        BasicCredentials credentials = BasicCredentials.parse(getBody(exchange));
        if (credentials == null) {
            HttpUtils.sendEmptyRsp(exchange, 422);
            return;
        }
        usersDao.upsert(credentials.username(), credentials.password());
        HttpUtils.sendEmptyRsp(exchange, 200);

    }

    private static boolean isValidId(String id) {
        return id != null && ID_PATTERN.matcher(id).matches();
    }

    private static boolean isValidLink(String link) {
        try {
            URI uri = URI.create(link);
            return uri.getScheme() != null && uri.getHost() != null;
        } catch (Exception e) {
            return false;
        }
    }

    private static String getBody(HttpExchange exchange) throws IOException {
        return new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
    }

    private static String generateShortId() {
        return UUID.randomUUID().toString().replace("-", "").substring(0, 10);
    }

}
