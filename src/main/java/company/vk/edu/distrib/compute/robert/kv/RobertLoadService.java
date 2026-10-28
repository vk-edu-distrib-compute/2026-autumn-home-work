package company.vk.edu.distrib.compute.robert.kv;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantLock;

import com.sun.net.httpserver.HttpServer;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.HttpService;
import company.vk.edu.distrib.compute.robert.api.v0.StatusHandler;
import company.vk.edu.distrib.compute.robert.kv.api.v0.EntityHandler;

public class RobertLoadService implements HttpService {
    private final int port;
    private final Dao<byte[]> dao;
    private final HttpServer httpServer;
    private final ExecutorService executor;
    private final ReentrantLock lock = new ReentrantLock();
    private ServiceState state = ServiceState.NEW;

    public RobertLoadService(int inputPort, Dao<byte[]> inputDao, int executorThreads) throws IOException {
        port = inputPort;
        dao = Objects.requireNonNull(inputDao);
        httpServer = HttpServer.create();
        httpServer.createContext(StatusHandler.PATH, new StatusHandler());
        httpServer.createContext(EntityHandler.PATH, new EntityHandler(dao));
        executor = Executors.newFixedThreadPool(executorThreads);
        httpServer.setExecutor(executor);
    }

    @Override
    public void start() {
        lock.lock();
        try {
            if (state != ServiceState.NEW) {
                throw new IllegalStateException();
            }

            try {
                httpServer.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), port), 0);
                httpServer.start();
                state = ServiceState.STARTED;
            } catch (IOException e) {
                state = ServiceState.STOPPED;
                httpServer.stop(0);
                executor.shutdownNow();
                try {
                    dao.close();
                } catch (IOException closeException) {
                    e.addSuppressed(closeException);
                }
                throw new UncheckedIOException(e);
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void stop() {
        lock.lock();
        try {
            if (state != ServiceState.STARTED) {
                throw new IllegalStateException();
            }

            state = ServiceState.STOPPED;
            try {
                httpServer.stop(1);
            } finally {
                executor.shutdownNow();
                closeDao();
            }
        } finally {
            lock.unlock();
        }
    }

    private void closeDao() {
        try {
            dao.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private enum ServiceState {
        NEW,
        STARTED,
        STOPPED
    }
}
