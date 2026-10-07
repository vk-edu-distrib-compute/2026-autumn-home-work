package company.vk.edu.distrib.compute.iizhukov.urlshortener;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nullable;

import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.iizhukov.shared.http.BaseController;
import company.vk.edu.distrib.compute.iizhukov.shared.http.Middleware;
import company.vk.edu.distrib.compute.iizhukov.urlshortener.api.helpers.middlewares.ErrorHandlingMiddleware;
import company.vk.edu.distrib.compute.iizhukov.urlshortener.api.v0.IndexController;
import company.vk.edu.distrib.compute.iizhukov.urlshortener.api.v0.InternalUsersController;
import company.vk.edu.distrib.compute.iizhukov.urlshortener.api.v0.LinksController;
import company.vk.edu.distrib.compute.iizhukov.urlshortener.api.v0.StatusController;
import company.vk.edu.distrib.compute.urlshortener.UrlShortenerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Application implements UrlShortenerService {
    private static final Logger log = LoggerFactory.getLogger(Application.class);

    private final List<Middleware> middlewares = List.of(new ErrorHandlingMiddleware());

    private boolean used;

    @Nullable
    private HttpServer server;
    @Nullable
    private LinksController linksController;
    @Nullable
    private IndexController indexController;

    public void init(int port) throws IOException {
        server = HttpServer.create(new InetSocketAddress(port), 1);
        linksController = new LinksController(port);
        indexController = new IndexController(port);

        var registry = List.<BaseController<?>>of(
                new StatusController(port),
                linksController,
                indexController,
                new InternalUsersController(port)
        );

        registry.forEach(controller -> {
            Objects.requireNonNull(server).createContext(controller.path(), controller.handler(middlewares));
            if (log.isInfoEnabled()) { // codacy...
                log.info("Controller {} was registered", controller.getClass().getName());
            }
        });
    }

    @Override
    public void setLinksDao(Dao<String> dao) {
        if (used) {
            throw new IllegalStateException("Application has already been used");
        }

        Objects.requireNonNull(linksController).setDao(dao);
        Objects.requireNonNull(indexController).setDao(dao);
    }

    @Override
    public void start() {
        if (server == null) {
            throw new IllegalStateException("Application must be initialized");
        }

        used = true;
        server.start();
    }

    @Override
    public void stop() {
        used = true;

        if (server == null) {
            return;
        }

        server.stop(1);
    }
}
