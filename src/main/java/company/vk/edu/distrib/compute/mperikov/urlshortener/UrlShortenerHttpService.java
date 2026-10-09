package company.vk.edu.distrib.compute.mperikov.urlshortener;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;

import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.urlshortener.UrlShortenerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class UrlShortenerHttpService implements UrlShortenerService {
    private static final Logger log = LoggerFactory.getLogger(UrlShortenerHttpService.class);

    private final int port;
    private final HttpServer server;
    private final LinkRequests linkRequests;
    private final Dao<String> users;
    private Dao<String> links;
    private boolean started;
    private boolean daoFixed;

    public UrlShortenerHttpService(int port, Dao<String> links, Dao<String> users) throws IOException {
        this.port = port;
        this.links = links;
        this.users = users;
        linkRequests = new LinkRequests(port, links);
        server = HttpServer.create();
        server.createContext("/", new UrlShortenerHandler(linkRequests, users));
    }

    @Override
    public void setLinksDao(Dao<String> dao) {
        if (daoFixed) {
            throw new IllegalStateException("Links dao cannot be changed after start or stop");
        }
        if (links != dao) {
            closeDao(links);
        }
        links = dao;
        linkRequests.use(dao);
    }

    @Override
    public void start() {
        if (started) {
            throw new IllegalStateException("Service is already started");
        }
        daoFixed = true;
        try {
            server.bind(new InetSocketAddress("localhost", port), 0);
            server.start();
        } catch (IOException ex) {
            throw new IllegalStateException("Failed to start URL shortener", ex);
        }
        started = true;
        log.info("URL shortener is listening on port {}", port);
    }

    @Override
    public void stop() {
        daoFixed = true;
        try {
            if (started) {
                server.stop(0);
            }
        } finally {
            closeDao(links);
            closeDao(users);
            started = false;
        }
        log.info("Stopped URL shortener on port {}", port);
    }

    private static void closeDao(Closeable dao) {
        try {
            dao.close();
        } catch (IOException ex) {
            log.warn("Failed to close storage", ex);
        }
    }
}
