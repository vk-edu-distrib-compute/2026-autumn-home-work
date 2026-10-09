package company.vk.edu.distrib.compute.virogg.urlshortener.links;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.urlshortener.UrlShortenerService;
import company.vk.edu.distrib.compute.virogg.http.HttpUtils;
import org.jspecify.annotations.Nullable;

public final class UrlShortenerServiceImpl implements UrlShortenerService {
    private static final String STATUS_PATH = "/v0/status";
    private static final String STATUS_KEY = "__urlshortener_status__";

    private final int port;
    private final Path dataDir;
    private final Lock lifecycle = new ReentrantLock();
    private boolean lifecycleStarted;
    private boolean linksInjected;
    @Nullable
    private Dao<String> links;
    @Nullable
    private PersistentDao users;
    @Nullable
    private HttpServer server;
    @Nullable
    private ExecutorService executor;

    public UrlShortenerServiceImpl(int port, Path dataDir) {
        this.port = port;
        this.dataDir = dataDir;
    }

    @Override
    public void setLinksDao(Dao<String> dao) {
        lifecycle.lock();
        try {
            if (lifecycleStarted) {
                throw new IllegalStateException("Service lifecycle has already started");
            }
            links = dao;
            linksInjected = true;
        } finally {
            lifecycle.unlock();
        }
    }

    @Override
    public void start() {
        lifecycle.lock();
        try {
            lifecycleStarted = true;
            if (server != null || linksInjected && links == null) {
                throw new IllegalStateException("Service is running or its injected DAO has been closed");
            }
            ExecutorService workers = Executors.newVirtualThreadPerTaskExecutor();
            executor = workers;
            server = openAndCreateServer(workers);
        } finally {
            lifecycle.unlock();
        }
    }

    @Override
    public void stop() {
        lifecycle.lock();
        try {
            lifecycleStarted = true;
            if (server != null) {
                server.stop(0);
                server = null;
            }
            releaseResources();
        } finally {
            lifecycle.unlock();
        }
    }

    private HttpServer openAndCreateServer(ExecutorService workers) {
        try {
            Dao<String> linksDao = links;
            if (linksDao == null) {
                linksDao = new PersistentDao(dataDir, "links.log");
            }
            links = linksDao;
            PersistentDao usersDao = new PersistentDao(dataDir, "users.log");
            users = usersDao;
            return createServer(linksDao, usersDao, workers);
        } catch (IOException e) {
            releaseResources();
            throw new UncheckedIOException("Failed to start urlshortener service on port " + port, e);
        } catch (RuntimeException e) {
            releaseResources();
            throw e;
        }
    }

    private HttpServer createServer(Dao<String> linksDao, PersistentDao usersDao, ExecutorService workers)
        throws IOException {
        HttpServer httpServer = HttpServer.create(new InetSocketAddress(port), 0);
        LinksHandler linksHandler = new LinksHandler(linksDao, port);
        httpServer.createContext(STATUS_PATH, HttpUtils.safe(exchange -> status(exchange, linksDao, usersDao)));
        httpServer.createContext(LinksHandler.BASE_PATH, HttpUtils.safe(linksHandler))
            .setAuthenticator(new UsersAuthenticator(usersDao));
        httpServer.createContext(UsersHandler.PATH, HttpUtils.safe(new UsersHandler(usersDao)));
        httpServer.createContext("/", HttpUtils.safe(linksHandler::redirect));
        httpServer.setExecutor(workers);
        httpServer.start();
        return httpServer;
    }

    private void releaseResources() {
        if (executor != null) {
            executor.close();
            executor = null;
        }
        try (Dao<String> _ = links) {
            if (users != null) {
                users.close();
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to close storage", e);
        } finally {
            links = null;
            users = null;
        }
    }

    private static void status(HttpExchange exchange, Dao<String> linksDao, PersistentDao usersDao)
        throws IOException {
        if (!HttpUtils.accepts(exchange, STATUS_PATH, HttpUtils.GET)) {
            return;
        }
        boolean healthy = usersDao.isWritable() && linksHealthy(linksDao);
        HttpUtils.sendEmpty(exchange, healthy ? HttpURLConnection.HTTP_OK : HttpURLConnection.HTTP_UNAVAILABLE);
    }

    private static boolean linksHealthy(Dao<String> linksDao) {
        if (linksDao instanceof PersistentDao persistent) {
            return persistent.isWritable();
        }
        try {
            linksDao.get(STATUS_KEY);
            return true;
        } catch (NoSuchElementException expected) {
            return true;
        } catch (IOException e) {
            return false;
        }
    }
}
