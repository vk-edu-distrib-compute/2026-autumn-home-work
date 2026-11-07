package company.vk.edu.distrib.compute.akravchenya.urlshortener;

import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.urlshortener.UrlShortenerService;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;

/**
 * {@link UrlShortenerService}, поддерживаемый {@link HttpServer}
 * c постоянными хранилищами {@link FileStringDao} для ссылок и пользователей.
 */
class UrlShortenerServiceImpl implements UrlShortenerService {

    private final HttpServer server;
    private final FileStringDao links;
    private final FileStringDao users;

    UrlShortenerServiceImpl(int port) throws IOException {
        this.server = HttpServer.create(new InetSocketAddress(port), 0);

        var storeDir = Files.createTempDirectory("urlshortener-" + port + "-");
        this.links = new FileStringDao(storeDir.resolve("links.snapshot"));
        this.users = new FileStringDao(storeDir.resolve("users.snapshot"));

        var authentication = new Authentication(users);
        server.createContext("/v0/status", new RequestHandler(new StatusHandler()));
        server.createContext("/v0/links", new RequestHandler(new LinksHandler(links, authentication, port)));
        server.createContext("/internal/users", new RequestHandler(new UsersHandler(users)));
        server.createContext("/", new RequestHandler(new RedirectHandler(links)));
    }

    @Override
    public void start() {
        server.start();
    }

    @Override
    public void stop() {
        try {
            links.close();
            users.close();
        } catch (IOException exception) {
            throw new UncheckedIOException(exception);
        } finally {
            server.stop(1);
        }
    }
}
