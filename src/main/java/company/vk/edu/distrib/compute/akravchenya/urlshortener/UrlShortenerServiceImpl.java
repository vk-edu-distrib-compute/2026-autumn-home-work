package company.vk.edu.distrib.compute.akravchenya.urlshortener;

import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.urlshortener.UrlShortenerService;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.time.Duration;

/**
 * {@link UrlShortenerService}, поддерживаемый {@link HttpServer}
 * c постоянными хранилищами {@link FileStringDao} для ссылок и пользователей.
 */
class UrlShortenerServiceImpl implements UrlShortenerService {

    private static final Duration BIND_TIMEOUT = Duration.ofSeconds(3);
    private static final Duration BIND_RETRY_DELAY = Duration.ofMillis(100);

    private final HttpServer server;
    private final LinksHandler linksHandler;
    private final RedirectHandler redirectHandler;
    private final FileStringDao localLinks;
    private final FileStringDao users;
    private KafkaModerationConnector moderationConnector;
    private boolean used;

    UrlShortenerServiceImpl(int port) throws IOException {
        this.server = createServer(port);

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
    public void setKafkaBootstrapServers(String bootstrapServers) {
        if (used) {
            throw new IllegalStateException("kafka bootstrap servers must be set before the service is started");
        }
        moderationConnector = new KafkaModerationConnector(bootstrapServers, linksHandler::removeLink);
        linksHandler.setModeration(moderationConnector);
    }

    @Override
    public void start() {
        if (used) {
            throw new IllegalStateException("service is already started");
        }
        used = true;
        if (moderationConnector != null) {
            moderationConnector.start();
        }
        server.start();
    }

    @Override
    public void stop() {
        if (!used) {
            throw new IllegalStateException("service is not started");
        }
        try {
            if (moderationConnector != null) {
                moderationConnector.close();
            }
            localLinks.close();
            users.close();
        } catch (IOException exception) {
            throw new UncheckedIOException(exception);
        } finally {
            server.stop(1);
        }
    }

    private static HttpServer createServer(int port) throws IOException {
        var address = new InetSocketAddress(port);
        var deadline = System.nanoTime() + BIND_TIMEOUT.toNanos();
        while (true) {
            try {
                return HttpServer.create(address, 0);
            } catch (BindException exception) {
                if (System.nanoTime() >= deadline) {
                    throw exception;
                }
                try {
                    Thread.sleep(BIND_RETRY_DELAY.toMillis());
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    exception.addSuppressed(interrupted);
                    throw exception;
                }
            }
        }
    }
}
