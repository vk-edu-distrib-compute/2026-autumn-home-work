package company.vk.edu.distrib.compute.virogg.kv;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantLock;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.kv.KVService;
import company.vk.edu.distrib.compute.virogg.http.HttpUtils;
import org.jspecify.annotations.Nullable;

public final class KVServiceImpl implements KVService {
    private final int port;
    private final Path directory;
    private final boolean multithreaded;
    private final ReentrantLock reentrantLock = new ReentrantLock();
    private boolean startAttempted;
    @Nullable
    private PersistentByteDao dao;
    @Nullable
    private HttpServer server;
    @Nullable
    private ExecutorService executor;

    public KVServiceImpl(int port, Path directory, boolean multithreaded) {
        this.port = port;
        this.directory = directory;
        this.multithreaded = multithreaded;
    }

    @Override
    public void start() {
        reentrantLock.lock();
        try {
            if (startAttempted) {
                throw new IllegalStateException("KV Service has already started");
            }
            startAttempted = true;
            try {
                PersistentByteDao storage = new PersistentByteDao(directory);
                dao = storage;
                HttpServer httpServer = HttpServer.create(new InetSocketAddress(port), 0);
                server = httpServer;
                httpServer.createContext(KvApiConstants.ENTITY_PATH,
                        HttpUtils.safe(new KVEntityHandler(storage)));
                httpServer.createContext(KvApiConstants.STATUS_PATH,
                        HttpUtils.safe(exchange -> status(exchange, storage)));
                httpServer.createContext("/", HttpUtils.safe(exchange ->
                        HttpUtils.sendEmpty(exchange, HttpURLConnection.HTTP_NOT_FOUND)));
                if (multithreaded) {
                    ExecutorService workers = Executors.newVirtualThreadPerTaskExecutor();
                    executor = workers;
                    httpServer.setExecutor(workers);
                }
                httpServer.start();
            } catch (IOException e) {
                releaseOnFailure(e);
                throw new UncheckedIOException("Failed to start KV service on port " + port, e);
            } catch (RuntimeException e) {
                releaseOnFailure(e);
                throw e;
            }
        } finally {
            reentrantLock.unlock();
        }
    }

    @Override
    public void stop() {
        reentrantLock.lock();
        try {
            if (!startAttempted) {
                throw new IllegalStateException("KV Service has not been started yet");
            }
            try {
                releaseResources();
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to close KV storage", e);
            }
        } finally {
            reentrantLock.unlock();
        }
    }

    private void releaseOnFailure(Exception failure) {
        try {
            releaseResources();
        } catch (IOException | RuntimeException closeFailure) {
            failure.addSuppressed(closeFailure);
        }
    }

    private void releaseResources() throws IOException {
        try (PersistentByteDao _ = dao; ExecutorService _ = executor) {
            if (server != null) {
                server.stop(0);
            }
        } finally {
            server = null;
            executor = null;
            dao = null;
        }
    }

    private static void status(HttpExchange exchange, PersistentByteDao storage) throws IOException {
        if (HttpUtils.accepts(exchange, KvApiConstants.STATUS_PATH, HttpUtils.GET)) {
            HttpUtils.sendEmpty(exchange,
                    storage.isWritable() ? HttpURLConnection.HTTP_OK : HttpURLConnection.HTTP_UNAVAILABLE);
        }
    }
}
