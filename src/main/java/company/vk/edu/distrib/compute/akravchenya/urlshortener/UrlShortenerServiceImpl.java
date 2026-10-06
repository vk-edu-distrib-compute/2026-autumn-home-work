package company.vk.edu.distrib.compute.akravchenya.urlshortener;

import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
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
    private final LinksHandler linksHandler;
    private final RedirectHandler redirectHandler;
    private final FileStringDao localLinks;
    private final FileStringDao users;
    private boolean used;

    UrlShortenerServiceImpl(int port) throws IOException {
        this.server = HttpServer.create(new InetSocketAddress(port), 0);

        var storeDir = Files.createTempDirectory("urlshortener-" + port + "-");
        this.localLinks = new FileStringDao(storeDir.resolve("links.snapshot"));
        this.users = new FileStringDao(storeDir.resolve("users.snapshot"));

        var authentication = new Authentication(users);
        this.linksHandler = new LinksHandler(localLinks, authentication, port);
        this.redirectHandler = new RedirectHandler(localLinks);

        server.createContext("/v0/status", new RequestHandler(new StatusHandler()));
        server.createContext("/v0/links", new RequestHandler(linksHandler));
        server.createContext("/internal/users", new RequestHandler(new UsersHandler(users)));
        server.createContext("/", new RequestHandler(redirectHandler));
    }

    @Override
    public void setLinksDao(Dao<String> dao) {
        if (used) {
            throw new IllegalStateException("links dao must be set before the service is started");
        }
        linksHandler.setDao(dao);
        redirectHandler.setDao(dao);
    }

    @Override
    public void start() {
        if (used) {
            throw new IllegalStateException("service is already started");
        }
        used = true;
        server.start();
    }

    @Override
    public void stop() {
        if (!used) {
            throw new IllegalStateException("service is not started");
        }
        try {
            localLinks.close();
            users.close();
        } catch (IOException exception) {
            throw new UncheckedIOException(exception);
        } finally {
            server.stop(1);
        }
    }
}
