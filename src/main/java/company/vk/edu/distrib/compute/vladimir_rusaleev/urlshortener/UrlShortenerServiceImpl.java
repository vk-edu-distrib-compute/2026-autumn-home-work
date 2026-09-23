package company.vk.edu.distrib.compute.vladimir_rusaleev.urlshortener;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.urlshortener.UrlShortenerService;

public final class UrlShortenerServiceImpl implements UrlShortenerService {
    private HttpServer server;
    private final Dao<String> links;
    private final Dao<String> users;
    private final UrlLinks urlLink;
    private final UserAuth userAuth;
    private final int port;

    public UrlShortenerServiceImpl(int port) throws IOException {
        Path root = Path.of(System.getProperty("user.home"), ".vk-urlshortener", "vladimir");
        links = new FileDao(root.resolve(Integer.toString(port)).resolve("links"));
        users = new FileDao(root.resolve(Integer.toString(port)).resolve("users"));
        urlLink = new UrlLinks(links, port);
        userAuth = new UserAuth(users);
        this.port = port;
    }

    @Override
    public void start() {
        try {
            server = HttpServer.create(new InetSocketAddress("localhost", port), 0);
            server.createContext("/", this::handle);
            server.start();
        } catch (IOException exception) {
            throw new UncheckedIOException("Could not start server", exception);
        }
    }

    @Override
    public void stop() {
        if (server != null) {
            server.stop(1);
        }
        try {
            links.close();
            users.close();
        } catch (IOException exception) {
            throw new IllegalStateException("Could not close stores", exception);
        }
    }

    private void handle(HttpExchange exchange) throws IOException {
        try (exchange) {
            try {
                route(exchange);
            } catch (IOException exception) {
                UrlLinks.respond(exchange, 503, null);
            }
        }
    }

    private void route(HttpExchange exchange) throws IOException {
        String path = exchange.getRequestURI().getPath();
        String method = exchange.getRequestMethod();
        if ("/v0/status".equals(path)) {
            UrlLinks.respond(exchange, "GET".equals(method) ? 200 : 405, null);
            return;
        }
        if ("/internal/users".equals(path)) {
            userAuth.createUser(exchange);
            return;
        }
        if (path.startsWith("/v0/links") && !userAuth.authenticated(exchange)) {
            exchange.getResponseHeaders().set("WWW-Authenticate", "Basic realm=\"urlshortener\", charset=\"UTF-8\"");
            UrlLinks.respond(exchange, 401, null);
            return;
        }
        if ("/v0/links".equals(path)) {
            if ("POST".equals(method)) {
                urlLink.createLink(exchange);
            } else {
                UrlLinks.respond(exchange, 405, null);
            }
            return;
        }
        if (path.startsWith("/v0/links/")) {
            urlLink.linkById(exchange, path.substring("/v0/links/".length()));
            return;
        }
        if (path.startsWith("/") && path.indexOf('/', 1) == -1 && "GET".equals(method)) {
            urlLink.redirect(exchange, path.substring(1));
            return;
        }
        UrlLinks.respond(exchange, 404, null);
    }
}
