package company.vk.edu.distrib.compute.mperikov.kv;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.http.HttpClient;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.kv.KVService;
import org.jspecify.annotations.Nullable;

final class KvHttpService implements KVService {
    static final String WORKERS_PROPERTY = "mperikov.kv.workers";
    private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(1);
    private static final String VIRTUAL_WORKERS = "virtual";

    private final int port;
    private final HttpServer server;
    private final Dao<byte[]> entities;
    private final HttpClient client;
    private @Nullable ExecutorService workers;
    private boolean started;
    private boolean clientClosed;

    KvHttpService(int port, Dao<byte[]> entities) throws IOException {
        this.port = port;
        this.entities = entities;
        client = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .connectTimeout(CONNECT_TIMEOUT)
            .build();
        server = HttpServer.create();
        server.createContext("/", new KvHandler(port, entities, client));
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
            closeClient();
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
            closeClient();
            closeEntities();
            started = false;
        }
    }

    private void attachWorkers() {
        String raw = System.getProperty(WORKERS_PROPERTY);
        if (raw == null || raw.isBlank()) {
            attachClusterWorkers();
            return;
        }
        workers = executor(raw.trim());
        server.setExecutor(workers);
    }

    private void attachClusterWorkers() {
        if (KvCodes.memberPorts().length == 0) {
            return;
        }
        workers = Executors.newVirtualThreadPerTaskExecutor();
        server.setExecutor(workers);
    }

    private static ExecutorService executor(String raw) {
        if (VIRTUAL_WORKERS.equalsIgnoreCase(raw)) {
            return Executors.newVirtualThreadPerTaskExecutor();
        }
        return Executors.newFixedThreadPool(Integer.parseInt(raw));
    }

    private void stopWorkers() {
        ExecutorService pool = workers;
        if (pool == null) {
            return;
        }
        pool.shutdown();
    }

    private void closeClient() {
        if (clientClosed) {
            return;
        }
        client.close();
        clientClosed = true;
    }

    private void closeEntities() {
        try {
            entities.close();
        } catch (IOException ex) {
            throw new IllegalStateException("Failed to close KV storage", ex);
        }
    }
}
