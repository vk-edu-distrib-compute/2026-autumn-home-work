package company.vk.edu.distrib.compute.akravchenya.kv;

import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.kv.KVService;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * {@link KVService}, поддерживаемый {@link HttpServer} и хранящий данные
 * на диске через {@link FileByteDao}.
 */
final class KeyValueServiceImpl implements KVService {

    private static final String WORKER_THREADS_PROPERTY = "kv.workerThreads";

    private final HttpServer server;
    private ExecutorService executor;
    private boolean used;

    KeyValueServiceImpl(int port) throws IOException {
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        var storeDir = Files.createTempDirectory("kv-" + port + "-");
        var dao = new FileByteDao(storeDir);

        server.createContext("/v0/status", new KVRequestHandler(new StatusHandler()));
        server.createContext("/v0/entity", new KVRequestHandler(new EntityHandler(dao)));
    }

    @Override
    public void start() {
        if (used) {
            throw new IllegalStateException("service is already started");
        }
        used = true;
        configureExecutor();
        server.start();
    }

    @Override
    public void stop() {
        if (!used) {
            throw new IllegalStateException("service is not started");
        }
        try {
            if (executor != null) {
                executor.shutdownNow();
            }
        } finally {
            server.stop(1);
        }
    }

    private void configureExecutor() {
        var workersProperty = System.getProperty(WORKER_THREADS_PROPERTY);
        if (workersProperty == null || workersProperty.isBlank()) {
            return;
        }
        var workers = Integer.parseInt(workersProperty);
        this.executor = Executors.newFixedThreadPool(workers);
        server.setExecutor(executor);
    }
}
