package company.vk.edu.distrib.compute.iizhukov.kv;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.iizhukov.kv.api.helpers.middlewares.ErrorHandlingMiddleware;
import company.vk.edu.distrib.compute.iizhukov.kv.api.v0.EntityController;
import company.vk.edu.distrib.compute.iizhukov.kv.api.v0.StatusController;
import company.vk.edu.distrib.compute.iizhukov.shared.http.BaseController;
import company.vk.edu.distrib.compute.iizhukov.shared.http.Middleware;
import company.vk.edu.distrib.compute.kv.KVService;

public final class Application implements KVService {
    private final List<Middleware> middlewares = List.of(new ErrorHandlingMiddleware());
    private final HttpServer server;
    private final FileDao dao;
    private final ExecutorService executor;

    public Application(int port) throws IOException {
        this(port, 1, Path.of("/tmp/iizhukov-kv/data.db"));
    }

    public Application(int port, int threads, Path dataPath) throws IOException {
        executor = Executors.newFixedThreadPool(threads);
        server = HttpServer.create(new InetSocketAddress(port), 1);
        dao = new FileDao(dataPath);
        server.setExecutor(executor);

        var registry = List.<BaseController<?>>of(
                new StatusController(dao::isOpen),
                new EntityController(dao)
        );

        registry.forEach(controller -> server.createContext(
                controller.path(),
                controller.handler(middlewares)
        ));
    }

    @Override
    public void start() {
        server.start();
    }

    @Override
    public void stop() {
        server.stop(1);

        executor.close();

        try {
            dao.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
