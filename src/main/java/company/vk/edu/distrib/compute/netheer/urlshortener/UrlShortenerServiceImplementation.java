package company.vk.edu.distrib.compute.netheer.urlshortener;

import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.urlshortener.UrlShortenerService;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class UrlShortenerServiceImplementation implements UrlShortenerService {
    private Dao<String> linksDao;
    private final Dao<String> usersDao;
    private final HttpServer server;
    private final UrlShortenerHandlers handlers;
    private final Lock lifecycleLock = new ReentrantLock();

    private boolean lifecycleStarted;

    public UrlShortenerServiceImplementation(
            int port,
            Dao<String> linksDao,
            Dao<String> usersDao
    ) throws IOException {
        this.linksDao = linksDao;
        this.usersDao = usersDao;

        UrlShortenerAuth auth = new UrlShortenerAuth(usersDao);
        this.handlers = new UrlShortenerHandlers(port, linksDao, auth);

        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/v0/status", handlers::handleStatus);
        server.createContext("/v0/links", handlers::handleLinks);
        server.createContext("/internal/users", auth::handleUsers);
        server.createContext("/", handlers::handleRedirect);
    }

    @Override
    public void setLinksDao(Dao<String> dao) {
        lifecycleLock.lock();
        try {
            if (lifecycleStarted) {
                throw new IllegalStateException(
                        "Links DAO can't be changed after service start"
                );
            }

            linksDao = dao;
            handlers.setLinksDao(dao);
        } finally {
            lifecycleLock.unlock();
        }
    }

    @Override
    public void start() {
        lifecycleLock.lock();
        try {
            lifecycleStarted = true;
            server.start();
        } finally {
            lifecycleLock.unlock();
        }
    }

    @Override
    public void stop() {
        lifecycleLock.lock();
        try {
            Dao<String> currentLinksDao = linksDao;

            try (currentLinksDao; usersDao) {
                server.stop(0);
            } catch (IOException exception) {
                throw new UncheckedIOException(exception);
            }
        } finally {
            lifecycleLock.unlock();
        }
    }
}
