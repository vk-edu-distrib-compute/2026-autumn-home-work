package company.vk.edu.distrib.compute.allpandasarecute.urlshortener;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

import com.sun.net.httpserver.HttpServer;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.allpandasarecute.common.AbstractHttpService;
import company.vk.edu.distrib.compute.urlshortener.UrlShortenerService;

public class UrlShortenerServiceImpl extends AbstractHttpService implements UrlShortenerService {
    private static final String STATUS_PATH = "/v0/status";
    private static final String LINKS_PATH = "/v0/links";
    private static final String USERS_PATH = "/internal/users";

    private final AtomicReference<Dao<String>> links = new AtomicReference<>();

    public UrlShortenerServiceImpl(int port, Dao<String> links, Dao<String> users) throws IOException {
        super(port);
        this.links.set(links);
        HttpServer server = server();
        server.createContext(STATUS_PATH, new StatusHandler());
        server.createContext(USERS_PATH, new UsersHandler(users));
        server.createContext(LINKS_PATH, new LinksHandler(port, this::linksDao, new BasicAuthenticator(users)));
        server.createContext("/", new RedirectHandler(this::linksDao));
    }

    @Override
    public void setLinksDao(Dao<String> dao) {
        if (isStarted() || isStopped()) {
            throw new IllegalStateException("Can not change links dao after start or stop");
        }
        links.set(dao);
    }

    private Dao<String> linksDao() {
        return links.get();
    }
}
