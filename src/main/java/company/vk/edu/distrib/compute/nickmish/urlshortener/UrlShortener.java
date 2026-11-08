package company.vk.edu.distrib.compute.nickmish.urlshortener;

import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.urlshortener.UrlShortenerService;
import org.jspecify.annotations.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public final class UrlShortener implements UrlShortenerService {
    private final int port;
    private final Lock lock = new ReentrantLock();
    private boolean startCalled;
    private boolean stopCalled;
    @Nullable private HttpServer server;
    @Nullable private ExecutorService executor;
    @Nullable private FileStringDao linkDao;
    @Nullable private FileStringDao userDao;
    @Nullable private Dao<String> injectedLinksDao;

    public UrlShortener(int port) {
        this.port = port;
    }

    @Override
    public void setLinksDao(Dao<String> dao) {
        lock.lock();
        try {
            if (startCalled || stopCalled) {
                throw new IllegalStateException("Links dao can only be injected before start");
            }
            injectedLinksDao = dao;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void start() {
        lock.lock();
        try {
            if (startCalled) {
                throw new IllegalStateException("Service can only be started once");
            }
            startCalled = true;
            try {
                Path dataDir = Files.createTempDirectory("nickmish-urlshortener");
                FileStringDao links = new FileStringDao(dataDir.resolve("links"));
                linkDao = links;
                FileStringDao users = new FileStringDao(dataDir.resolve("users"));
                userDao = users;
                HttpServer httpServer = HttpServer.create(new InetSocketAddress("localhost", port), 0);
                server = httpServer;
                ExecutorService workers = Executors.newVirtualThreadPerTaskExecutor();
                executor = workers;
                httpServer.setExecutor(workers);
                Dao<String> linksStorage = injectedLinksDao != null ? injectedLinksDao : links;
                httpServer.createContext("/", new UrlShortenerHandler(
                        port,
                        new LinkService(linksStorage),
                        new UserService(users),
                        new BasicAuthenticator(users),
                        () -> links.isAvailable() && users.isAvailable()));
                httpServer.start();
            } catch (IOException | RuntimeException e) {
                try {
                    stop();
                } catch (RuntimeException ee) {
                    e.addSuppressed(ee);
                }
                throw new IllegalStateException("Cannot start URL shortener on port " + port, e);
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void stop() {
        lock.lock();
        try {
            if (!startCalled || stopCalled) {
                return;
            }
            stopCalled = true;
            closeResources();
        } finally {
            lock.unlock();
        }
    }

    private void closeResources() {
        FileStringDao links = linkDao;
        FileStringDao users = userDao;
        ExecutorService workers = executor;
        try (links;
                users;
                workers) {
            HttpServer httpServer = server;
            if (httpServer != null) {
                httpServer.stop(1);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot close URL shortener storage", e);
        }
    }
}
