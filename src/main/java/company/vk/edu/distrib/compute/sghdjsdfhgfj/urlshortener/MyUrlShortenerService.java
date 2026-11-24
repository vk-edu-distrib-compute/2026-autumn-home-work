package company.vk.edu.distrib.compute.sghdjsdfhgfj.urlshortener;

import com.sun.net.httpserver.HttpContext;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.masha533.urlshortener.PersistentDao;
import company.vk.edu.distrib.compute.sghdjsdfhgfj.CustomHttpHandler;
import company.vk.edu.distrib.compute.sghdjsdfhgfj.CustomHttpHandlerTranslator;
import company.vk.edu.distrib.compute.sghdjsdfhgfj.StatusCodeException;
import company.vk.edu.distrib.compute.sghdjsdfhgfj.urlshortener.handlers.LinksHandler;
import company.vk.edu.distrib.compute.sghdjsdfhgfj.urlshortener.handlers.RedirectHandler;
import company.vk.edu.distrib.compute.sghdjsdfhgfj.urlshortener.handlers.StatusHandler;
import company.vk.edu.distrib.compute.sghdjsdfhgfj.urlshortener.handlers.UsersHandler;
import company.vk.edu.distrib.compute.urlshortener.UrlShortenerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.NoSuchElementException;
import java.util.concurrent.Executors;

public class MyUrlShortenerService implements UrlShortenerService {
    private final HttpServer server;
    private Dao<String> urls;
    private final Dao<String> users;
    private static final int TWO = 2;
    private static final Logger LOGGER = LoggerFactory.getLogger(MyUrlShortenerService.class);

    public MyUrlShortenerService(int port) throws IOException {
        InetSocketAddress addr = new InetSocketAddress(port);
        server = HttpServer.create(addr, 0);
        server.setExecutor(Executors.newFixedThreadPool(8));
        Path tempDir = Files.createTempDirectory("sghdjsdfhgfj");
        urls = new PersistentDao(tempDir.resolve("urls.dat"));
        users = new PersistentDao(tempDir.resolve("users.dat"));

        addContext("/v0/status", new StatusHandler());
        addContext("/v0/links", new LinksHandler(this));
        addContext("/internal/users", new UsersHandler(this));
        addContext("/", new RedirectHandler(this));
    }

    @Override
    public void start() {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Starting MyUrlShortenerService at {}", server.getAddress());
        }
        server.start();
    }

    @Override
    public void stop() {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Stopping MyUrlShortenerService at {}", server.getAddress());
        }
        server.stop(0);
    }

    public String host() {
        return "http://localhost:" + server.getAddress().getPort();
    }

    private HttpContext addContext(String path, CustomHttpHandler handler) {
        return server.createContext(path, new CustomHttpHandlerTranslator(handler));
    }

    private boolean isAuthenticated(HttpExchange xch) throws IOException {
        String header = xch.getRequestHeaders().getFirst("Authorization");
        if (header == null) {
            return false;
        }
        if (!header.startsWith("Basic ")) {
            return false;
        }
        Base64.Decoder decoder = Base64.getDecoder();
        String auth = new String(decoder.decode(header.substring(6)), StandardCharsets.UTF_8);
        String[] credentials = auth.split(":");
        if (credentials.length != TWO) {
            return false;
        }
        try {
            String pass = users.get(credentials[0]);
            return pass.equals(credentials[1]);
        } catch (NoSuchElementException e) {
            return false;
        }
    }

    public void checkAuthentication(HttpExchange xch) throws IOException, StatusCodeException {
        if (!isAuthenticated(xch)) {
            throw StatusCodeException.unauthorized();
        }
    }

    public boolean isUrlRegistered(String id) throws IOException {
        try {
            urls.get(id);
            return true;
        } catch (NoSuchElementException e) {
            return false;
        }
    }

    public void upsertUrl(String id, String url) throws IOException {
        urls.upsert(id, url);
    }

    public String getLongUrl(String id) throws IOException {
        return urls.get(id);
    }

    public void deleteUrl(String id) throws IOException {
        urls.delete(id);
    }

    public void upsertUser(String username, String password) throws IOException {
        users.upsert(username, password);
    }

    @Override
    public void setLinksDao(Dao<String> dao) {
        urls = dao;
    }
}
