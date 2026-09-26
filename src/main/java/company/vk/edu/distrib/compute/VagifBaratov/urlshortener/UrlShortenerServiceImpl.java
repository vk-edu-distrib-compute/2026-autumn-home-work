package company.vk.edu.distrib.compute.vagifbaratov.urlshortener;

import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.vagifbaratov.urlshortener.handler.ErrorHandler;
import company.vk.edu.distrib.compute.vagifbaratov.urlshortener.handler.LinksHandler;
import company.vk.edu.distrib.compute.vagifbaratov.urlshortener.handler.RedirectHandler;
import company.vk.edu.distrib.compute.vagifbaratov.urlshortener.handler.StatusHandler;
import company.vk.edu.distrib.compute.urlshortener.UrlShortenerService;

import java.io.IOException;
import java.net.InetSocketAddress;

public class UrlShortenerServiceImpl implements UrlShortenerService {
    private final HttpServer server;
    private final Dao<String> dao = new InMemoryDao();

    public UrlShortenerServiceImpl(int port) throws IOException {
        this.server = HttpServer.create(new InetSocketAddress(port), 0);

        server.createContext(
                "/v0/status",
                new ErrorHandler(new StatusHandler()));

        server.createContext(
                "/v0/links",
                new ErrorHandler(new LinksHandler(port, dao)));

        server.createContext("/", new ErrorHandler(new RedirectHandler(dao)));
    }

    @Override
    public void start() {
        server.start();
    }

    @Override
    public void stop() {
        server.stop(1);
    }
}
