package company.vk.edu.distrib.compute.playingpeano.urlshortener;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.moderation.LinkCreatedEvent;
import company.vk.edu.distrib.compute.urlshortener.UrlShortenerService;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.security.SecureRandom;
import java.time.Instant;
import java.util.NoSuchElementException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.jspecify.annotations.Nullable;

import static company.vk.edu.distrib.compute.playingpeano.urlshortener.HttpUtils.hasTextContentType;
import static company.vk.edu.distrib.compute.playingpeano.urlshortener.HttpUtils.readBody;
import static company.vk.edu.distrib.compute.playingpeano.urlshortener.HttpUtils.sendEmpty;
import static company.vk.edu.distrib.compute.playingpeano.urlshortener.HttpUtils.sendMethodNotAllowed;

final class UrlShortenerServiceImpl implements UrlShortenerService {
    private static final String LINKS_PATH = "/v0/links";
    private static final String LINK_PATH_PREFIX = LINKS_PATH + "/";
    private static final String USERS_PATH = "/internal/users";
    private static final String STATUS_PATH = "/v0/status";
    private static final String LOCALHOST = "localhost";
    private static final String SHORT_LINK_PREFIX = "http://" + LOCALHOST + ":";
    private static final String LOCATION_HEADER = "Location";
    private static final String GET_METHOD = "GET";
    private static final String POST_METHOD = "POST";
    private static final String PUT_METHOD = "PUT";
    private static final String DELETE_METHOD = "DELETE";
    private static final String ALLOWED_LINK_METHODS = GET_METHOD + ", " + PUT_METHOD + ", " + DELETE_METHOD;
    private static final int STOP_DELAY_SECONDS = 1;
    private static final int STATUS_OK = 200;
    private static final int STATUS_CREATED = 201;
    private static final int STATUS_ACCEPTED = 202;
    private static final int STATUS_MOVED_PERMANENTLY = 301;
    private static final int STATUS_NOT_FOUND = 404;
    private static final int STATUS_UNSUPPORTED_MEDIA_TYPE = 415;
    private static final int STATUS_UNPROCESSABLE_CONTENT = 422;
    private static final int STATUS_INTERNAL_SERVER_ERROR = 500;
    private static final int STATUS_SERVICE_UNAVAILABLE = 503;

    private final int port;
    private Dao<String> links;
    private final BasicAuthenticator authentication;
    private final HttpServer server;
    private final SecureRandom random = new SecureRandom();
    private final Lock lifecycleLock = new ReentrantLock();
    private final Lock linksLock = new ReentrantLock();
    private @Nullable String kafkaBootstrapServers;
    private @Nullable KafkaUrlShortenerClient kafkaClient;
    private boolean lifecycleStarted;

    UrlShortenerServiceImpl(int port, PersistentStringDao links, PersistentStringDao users) throws IOException {
        this.port = port;
        this.links = links;
        authentication = new BasicAuthenticator(users);
        server = HttpServer.create(new InetSocketAddress(LOCALHOST, port), 0);
        server.createContext("/", this::handle);
    }

    @Override
    public void start() {
        lifecycleLock.lock();
        try {
            lifecycleStarted = true;
            @Nullable String bootstrapServers = kafkaBootstrapServers;
            @Nullable KafkaUrlShortenerClient client = bootstrapServers == null
                ? null : new KafkaUrlShortenerClient(bootstrapServers, links, linksLock);
            kafkaClient = client;
            server.start();
            if (client != null) {
                client.start();
            }
        } finally {
            lifecycleLock.unlock();
        }
    }

    @Override
    public void stop() {
        lifecycleLock.lock();
        try {
            lifecycleStarted = true;
            Dao<String> linksToClose = links;
            try (linksToClose; authentication) {
                server.stop(STOP_DELAY_SECONDS);
                if (kafkaClient != null) {
                    kafkaClient.stop();
                }
            } catch (IOException exception) {
                throw new UncheckedIOException("Cannot close links DAO", exception);
            }
        } finally {
            lifecycleLock.unlock();
        }
    }

    @Override
    public void setKafkaBootstrapServers(String bootstrapServers) {
        lifecycleLock.lock();
        try {
            if (lifecycleStarted) {
                throw new IllegalStateException("Kafka can only be configured before service start");
            }
            if (bootstrapServers.isBlank()) {
                throw new IllegalArgumentException("Kafka bootstrap servers must not be blank");
            }
            kafkaBootstrapServers = bootstrapServers;
        } finally {
            lifecycleLock.unlock();
        }
    }

    @Override
    public void setLinksDao(Dao<String> dao) {
        lifecycleLock.lock();
        try {
            if (lifecycleStarted) {
                throw new IllegalStateException("Links DAO can only be set before service start");
            }
            Dao<String> previous = links;
            try (previous) {
                links = dao;
            } catch (IOException exception) {
                throw new UncheckedIOException("Cannot close previous links DAO", exception);
            }
        } finally {
            lifecycleLock.unlock();
        }
    }

    private void handle(HttpExchange exchange) throws IOException {
        try (exchange) {
            try {
                route(exchange);
            } catch (NoSuchElementException exception) {
                sendEmpty(exchange, STATUS_NOT_FOUND);
            } catch (IllegalArgumentException exception) {
                sendEmpty(exchange, STATUS_UNPROCESSABLE_CONTENT);
            } catch (IOException exception) {
                sendEmpty(exchange, STATUS_SERVICE_UNAVAILABLE);
            } catch (RuntimeException exception) {
                sendEmpty(exchange, STATUS_INTERNAL_SERVER_ERROR);
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
        sendEmpty(exchange, authentication.isAvailable() ? STATUS_OK : STATUS_SERVICE_UNAVAILABLE);
    }

    private void handleUsers(HttpExchange exchange, String method) throws IOException {
        if (!POST_METHOD.equals(method)) {
            sendMethodNotAllowed(exchange, POST_METHOD);
            return;
        }
        if (!hasTextContentType(exchange)) {
            sendEmpty(exchange, STATUS_UNSUPPORTED_MEDIA_TYPE);
            return;
        }

        authentication.register(readBody(exchange));
        sendEmpty(exchange, STATUS_OK);
    }

    private void handleLinkCollection(HttpExchange exchange, String method) throws IOException {
        if (!POST_METHOD.equals(method)) {
            sendMethodNotAllowed(exchange, POST_METHOD);
            return;
        }
        if (!hasTextContentType(exchange)) {
            sendEmpty(exchange, STATUS_UNSUPPORTED_MEDIA_TYPE);
            return;
        }

        String longLink = readBody(exchange);
        String id = createLink(longLink);
        HttpUtils.sendText(exchange, STATUS_CREATED, SHORT_LINK_PREFIX + port + "/" + id);
    }

    private void handleLink(HttpExchange exchange, String method, String id) throws IOException {
        HttpUtils.validateId(id);
        switch (method) {
            case GET_METHOD -> HttpUtils.sendText(exchange, STATUS_OK, links.get(id));
            case PUT_METHOD -> updateLink(exchange, id);
            case DELETE_METHOD -> deleteLink(exchange, id);
            default -> sendMethodNotAllowed(exchange, ALLOWED_LINK_METHODS);
        }
    }

    private void updateLink(HttpExchange exchange, String id) throws IOException {
        if (!hasTextContentType(exchange)) {
            sendEmpty(exchange, STATUS_UNSUPPORTED_MEDIA_TYPE);
            return;
        }
        String longLink = readBody(exchange);
        HttpUtils.validateLongLink(longLink);
        linksLock.lock();
        try {
            links.get(id);
            links.upsert(id, longLink);
        } finally {
            linksLock.unlock();
        }
        sendEmpty(exchange, STATUS_OK);
    }

    private void deleteLink(HttpExchange exchange, String id) throws IOException {
        linksLock.lock();
        try {
            links.delete(id);
        } finally {
            linksLock.unlock();
        }
        sendEmpty(exchange, STATUS_ACCEPTED);
    }

    private void handleRedirect(HttpExchange exchange, String method, String path) throws IOException {
        if (!GET_METHOD.equals(method)) {
            sendMethodNotAllowed(exchange, GET_METHOD);
            return;
        }
        if (path.length() <= 1 || path.charAt(0) != '/') {
            sendEmpty(exchange, STATUS_NOT_FOUND);
            return;
        }
        String id = path.substring(1);
        HttpUtils.validateId(id);
        exchange.getResponseHeaders().set(LOCATION_HEADER, links.get(id));
        sendEmpty(exchange, STATUS_MOVED_PERMANENTLY);
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
        linksLock.lock();
        try {
            do {
                id = HttpUtils.randomId(random);
            } while (linkExists(id));
            links.upsert(id, longLink);
        } finally {
            linksLock.unlock();
        }
        if (kafkaClient != null) {
            kafkaClient.publishCreated(new LinkCreatedEvent(id, longLink, Instant.now()));
        }
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
