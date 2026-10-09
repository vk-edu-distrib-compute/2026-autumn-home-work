package company.vk.edu.distrib.compute.nickmish.kv;

import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.kv.KVService;
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

public final class KVServiceImpl implements KVService {
    private final int port;
    private final Lock lock = new ReentrantLock();
    private boolean startCalled;
    private boolean stopCalled;
    @Nullable private HttpServer server;
    @Nullable private ExecutorService executor;
    @Nullable private Dao<byte[]> dao;

    public KVServiceImpl(int port) {
        this.port = port;
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
                Path dataDir = Files.createTempDirectory("nickmish-kv");
                FileByteArrayDao dao = new FileByteArrayDao(dataDir);
                this.dao = dao;
                HttpServer httpServer = HttpServer.create(new InetSocketAddress("localhost", port), 0);
                server = httpServer;
                ExecutorService workers = Executors.newVirtualThreadPerTaskExecutor();
                executor = workers;
                httpServer.setExecutor(workers);
                httpServer.createContext("/", new KVHandler(dao, dao::isAvailable));
                httpServer.start();
            } catch (IOException | RuntimeException e) {
                try {
                    stop();
                } catch (RuntimeException ee) {
                    e.addSuppressed(ee);
                }
                throw new IllegalStateException("Cannot start KV service on port " + port, e);
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
        Dao<byte[]> dao = this.dao;
        ExecutorService workers = executor;
        try (dao;
                workers) {
            HttpServer httpServer = server;
            if (httpServer != null) {
                httpServer.stop(1);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot close KV service storage", e);
        }
    }
}
