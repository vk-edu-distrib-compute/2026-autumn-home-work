package company.vk.edu.distrib.compute.fedorkhokhryakov.urlshortener;

import company.vk.edu.distrib.compute.urlshortener.UrlShortenerService;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Optional;

public class UrlShortenerServiceImpl implements UrlShortenerService {
private final int port;
private final InMemoryDao<String> dao;
private final InMemoryUserDao userDao;
private Optional<HttpServer> server = Optional.empty();

public UrlShortenerServiceImpl(int port, InMemoryDao<String> dao) {
    this(port, dao, new InMemoryUserDao());
}

public UrlShortenerServiceImpl(
    int port,
    InMemoryDao<String> dao,
    InMemoryUserDao userDao
) {
    this.port = port;
    this.dao = dao;
    this.userDao = userDao;
}

@Override
public void start() {
    if (server.isPresent()) {
        throw new IllegalStateException("Service has already been started");
    }

    try {
        HttpServer httpServer =
            HttpServer.create(new InetSocketAddress("localhost", port), 0);

        UrlShortenerHttpHandler handler =
            new UrlShortenerHttpHandler(port, dao, userDao);

        httpServer.createContext("/v0/status", handler::handleStatus);
        httpServer.createContext("/v0/links", handler::handleLinks);
        httpServer.createContext("/internal/users", handler::handleUsers);
        httpServer.createContext("/", handler::handleRedirect);

        httpServer.start();
        server = Optional.of(httpServer);
    } catch (IOException e) {
        throw new IllegalStateException("Failed to start HTTP server", e);
    }
}

@Override
public void stop() {
    HttpServer httpServer = server.orElseThrow(
        () -> new IllegalStateException("Service has not been started")
    );

    httpServer.stop(0);
    server = Optional.empty();
}

}
