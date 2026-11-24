package company.vk.edu.distrib.compute.kl1dd.urlshortener;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.urlshortener.UrlShortenerService;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.NoSuchElementException;

public class MyUrlShortenerService implements UrlShortenerService {
    private static final int STOP_DELAY_SECONDS = 2;
    private static final int SUPPOSED_LENGTH_AFTER_SPLIT = 2;
    private static final String GET_METHOD = "GET";
    private static final String POST_METHOD = "POST";
    private static final String LINKS_PATH = "/v0/links/";
    private static final String SLASH_STRING = "/";
    private static final String COLON_SPLITTER = ":";

    private final HttpServer httpServer;
    private Dao<String> linksDao;
    private final AuthService authService;
    private boolean startedOrStopped;

    private String kafkaBootstrapServers;
    private UrlModerationKafkaClient moderationClient;

    public MyUrlShortenerService(HttpServer httpServer) {
        this.httpServer = httpServer;
        this.linksDao = new MyDao();
        this.authService = new AuthService();

        httpServer.createContext("/v0/status", this::handleStatus);
        httpServer.createContext("/v0/links", this::handleLinks);
        httpServer.createContext("/", this::handleRedirect);
        httpServer.createContext("/internal/users", this::handleUsers);
    }

    @Override
    public void setLinksDao(Dao<String> dao) {
        if (startedOrStopped) {
            throw new IllegalStateException();
        }

        this.linksDao = dao;
    }

    private void handleUsers(HttpExchange exchange) throws IOException {
        String method = exchange.getRequestMethod();
        if (!(POST_METHOD.equals(method))) {
            exchange.sendResponseHeaders(405, -1);
            exchange.close();
            return;
        }

        String reqBody = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
        String[] reqSplit = reqBody.split(COLON_SPLITTER, 2);
        if (reqSplit.length != SUPPOSED_LENGTH_AFTER_SPLIT) {
            exchange.sendResponseHeaders(400, -1);
            exchange.close();
            return;
        }
        String username = reqSplit[0];
        String password = reqSplit[1];

        authService.createUser(username, password);

        exchange.sendResponseHeaders(200, -1);
        exchange.close();
    }

    private void handleStatus(HttpExchange exchange) throws IOException {
        if (!GET_METHOD.equals(exchange.getRequestMethod())) {
            exchange.sendResponseHeaders(405, -1);
            exchange.close();
            return;
        }

        exchange.sendResponseHeaders(200, -1);
        exchange.close();
    }

    private void handleLinks(HttpExchange exchange) throws IOException {
        if (!authService.isAuthed(exchange)) {
            exchange.sendResponseHeaders(401, -1);
            exchange.close();
            return;
        }

        String method = exchange.getRequestMethod();
        switch (method) {
            case "POST" -> {
                handlePost(exchange);
            }
            case "GET" -> {
                handleGet(exchange);
            }
            case "PUT" -> {
                handlePut(exchange);
            }
            case "DELETE" -> {
                handleDelete(exchange);
            }
            default -> {
                exchange.sendResponseHeaders(405, -1);
                exchange.close();
            }
        }
    }

    private void handlePost(HttpExchange exchange) throws IOException {
        String linkBefore = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
        if (!UrlShortenerUtils.isValidURL(linkBefore)) {
            exchange.sendResponseHeaders(422, -1);
            exchange.close();
            return;
        }

        String id = UrlShortenerUtils.generateRandomID(10);
        linksDao.upsert(id, linkBefore);
        if (moderationClient != null) {
            moderationClient.sendLinkCreated(id, linkBefore);
        }

        exchange.getResponseHeaders().set("Content-Type", "text/html; charset=utf-8");

        String linkAfter = "http://localhost:" + httpServer.getAddress().getPort() + "/" + id;
        byte[] responseBytes = linkAfter.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(201, responseBytes.length);
        exchange.getResponseBody().write(responseBytes);

        exchange.close();
    }

    private void handleGet(HttpExchange exchange) throws IOException {
        String reqPath = exchange.getRequestURI().getPath();
        String id = reqPath.substring(LINKS_PATH.length());

        if (!UrlShortenerUtils.isValidID(id)) {
            exchange.sendResponseHeaders(422, -1);
            exchange.close();
            return;
        }

        try {
            exchange.getResponseHeaders().set("Content-Type", "text/html; charset=utf-8");

            String linkByID = linksDao.get(id);
            byte[] responseBytes = linkByID.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, responseBytes.length);
            exchange.getResponseBody().write(responseBytes);
            exchange.close();
        } catch (NoSuchElementException e) {
            exchange.sendResponseHeaders(404, -1);
            exchange.close();
        }
    }

    private void handlePut(HttpExchange exchange) throws IOException {
        String reqPath = exchange.getRequestURI().getPath();
        String id = reqPath.substring(LINKS_PATH.length());
        if (!UrlShortenerUtils.isValidID(id)) {
            exchange.sendResponseHeaders(422, -1);
            exchange.close();
            return;
        }

        try {
            linksDao.get(id);

            String newLink = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            if (!UrlShortenerUtils.isValidURL(newLink)) {
                exchange.sendResponseHeaders(422, -1);
                exchange.close();
                return;
            }

            linksDao.upsert(id, newLink);

            exchange.sendResponseHeaders(200, -1);
            exchange.close();
        } catch (NoSuchElementException e) {
            exchange.sendResponseHeaders(404, -1);
            exchange.close();
        }
    }

    private void handleDelete(HttpExchange exchange) throws IOException {
        String reqPath = exchange.getRequestURI().getPath();
        String id = reqPath.substring(LINKS_PATH.length());

        if (!UrlShortenerUtils.isValidID(id)) {
            exchange.sendResponseHeaders(422, -1);
            exchange.close();
            return;
        }

        linksDao.delete(id);
        exchange.sendResponseHeaders(202, -1);
        exchange.close();
    }

    private void handleRedirect(HttpExchange exchange) throws IOException {
        String method = exchange.getRequestMethod();
        if (!GET_METHOD.equals(method)) {
            exchange.sendResponseHeaders(405, -1);
            exchange.close();
            return;
        }

        String reqPath = exchange.getRequestURI().getPath();
        String id = reqPath.substring(SLASH_STRING.length());
        if (!UrlShortenerUtils.isValidID(id)) {
            exchange.sendResponseHeaders(422, -1);
            exchange.close();
            return;
        }

        try {
            String linkByID = linksDao.get(id);

            exchange.getResponseHeaders().set("Location", linkByID);
            exchange.sendResponseHeaders(301, -1);
            exchange.close();
        } catch (NoSuchElementException e) {
            exchange.sendResponseHeaders(404, -1);
            exchange.close();
        }
    }

    @Override
    public void setKafkaBootstrapServers(String bootstrapServers) {
        if (startedOrStopped) {
            throw new IllegalStateException();
        }

        this.kafkaBootstrapServers = bootstrapServers;
    }

    @Override
    public void start() {
        startedOrStopped = true;
        if (kafkaBootstrapServers != null) {
            moderationClient = new UrlModerationKafkaClient(kafkaBootstrapServers,
                                                            httpServer.getAddress().getPort(),
                                                            id -> {
                                                                try {
                                                                    linksDao.delete(id);
                                                                } catch (IOException e) {
                                                                    throw new UncheckedIOException(e);
                                                                }
                                                            }
            );

            moderationClient.start();
        }

        httpServer.start();
    }

    @Override
    public void stop() {
        startedOrStopped = true;

        if (moderationClient != null) {
            moderationClient.stop();
        }

        httpServer.stop(STOP_DELAY_SECONDS);
    }
}
