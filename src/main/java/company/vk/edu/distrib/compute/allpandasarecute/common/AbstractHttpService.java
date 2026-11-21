package company.vk.edu.distrib.compute.allpandasarecute.common;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import com.sun.net.httpserver.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import company.vk.edu.distrib.compute.HttpService;

public abstract class AbstractHttpService implements HttpService {
    private static final Logger log = LoggerFactory.getLogger(AbstractHttpService.class);

    private static final int WORKER_THREADS = 4;
    private static final int BIND_ATTEMPTS = 5;
    private static final int BIND_RETRY_DELAY_MS = 100;

    private final int port;
    private final HttpServer server;
    private final ExecutorService executor;
    private final AtomicBoolean started = new AtomicBoolean();
    private final AtomicBoolean stopped = new AtomicBoolean();

    protected AbstractHttpService(int port) throws IOException {
        this.port = port;
        this.executor = Executors.newFixedThreadPool(WORKER_THREADS);
        this.server = HttpServer.create();
        server.setExecutor(executor);
    }

    protected final HttpServer server() {
        return server;
    }

    protected final boolean isStarted() {
        return started.get();
    }

    protected final boolean isStopped() {
        return stopped.get();
    }

    @Override
    public final void start() {
        if (!started.compareAndSet(false, true)) {
            throw new IllegalStateException("Service is already started");
        }
        bind();
        server.start();
        if (log.isInfoEnabled()) {
            log.info("{} is listening on port {}", getClass().getSimpleName(), port);
        }
    }

    @Override
    public final void stop() {
        if (!started.getAndSet(false)) {
            return;
        }
        stopped.set(true);
        server.stop(1);
        executor.shutdownNow();
        if (log.isInfoEnabled()) {
            log.info("{} on port {} is stopped", getClass().getSimpleName(), port);
        }
    }

    private void bind() {
        InetSocketAddress address = new InetSocketAddress(port);
        for (int attempt = 1; ; attempt++) {
            try {
                server.bind(address, 0);
                return;
            } catch (BindException e) {
                if (attempt == BIND_ATTEMPTS) {
                    throw new UncheckedIOException("Can not bind to port " + port, e);
                }
            } catch (IOException e) {
                throw new UncheckedIOException("Can not bind to port " + port, e);
            }
            sleepBeforeBindRetry();
        }
    }

    private static void sleepBeforeBindRetry() {
        try {
            Thread.sleep(BIND_RETRY_DELAY_MS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new UncheckedIOException(new IOException("Interrupted while binding", e));
        }
    }
}
