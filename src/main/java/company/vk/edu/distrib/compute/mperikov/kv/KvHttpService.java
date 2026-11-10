package company.vk.edu.distrib.compute.mperikov.kv;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.kv.KVService;
import org.jspecify.annotations.Nullable;

final class KvHttpService implements KVService {
    static final String WORKERS_PROPERTY = "mperikov.kv.workers";

    private final int port;
    private final HttpServer server;
    private final Dao<byte[]> entities;
    private @Nullable ExecutorService workers;
    private boolean started;

    KvHttpService(int port, Dao<byte[]> entities) throws IOException {
        this.port = port;
        this.entities = entities;
        server = HttpServer.create();
        server.createContext("/", new KvHandler(entities));
    }

    @Override
    public void start() {
        if (started) {
            throw new IllegalStateException("Service is already started");
        }
        attachWorkers();
        try {
            server.bind(new InetSocketAddress("localhost", port), 0);
            server.start();
        } catch (IOException ex) {
            throw new IllegalStateException("Failed to start KV service", ex);
        }
        started = true;
    }

    @Override
    public void stop() {
        try {
            if (started) {
                server.stop(0);
            }
        } finally {
            stopWorkers();
            closeEntities();
            started = false;
        }
    }

    private void attachWorkers() {
        String raw = System.getProperty(WORKERS_PROPERTY);
        if (raw == null || raw.isBlank()) {
            return;
        }
        workers = Executors.newFixedThreadPool(Integer.parseInt(raw));
        server.setExecutor(workers);
    }

    private void stopWorkers() {
        ExecutorService pool = workers;
        if (pool == null) {
            return;
        }
        pool.shutdown();
    }

    private void closeEntities() {
        try {
            entities.close();
        } catch (IOException ex) {
            throw new IllegalStateException("Failed to close KV storage", ex);
        }
    }
}
