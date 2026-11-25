package company.vk.edu.distrib.compute.annapiatkova.urlshortener;

import company.vk.edu.distrib.compute.urlshortener.UrlShortenerService;
import company.vk.edu.distrib.compute.Dao;

import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.net.InetAddress;
import java.io.IOException;

public class UrlShortenerServiceImpl implements UrlShortenerService {
    int port;
    HttpServer server;
    HandlerImpl handler;
    Dao<String> dao;
    boolean started;

    UrlShortenerServiceImpl(int port) throws IOException {
        server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), port), 0);
        this.port = port;
    }

    @Override
    public void start() {
        if (started) {
            throw new IllegalStateException("Service already started");
        }
        started = true;
        if (dao == null) {
            dao = new DaoImpl();
        }
        handler = new HandlerImpl(dao, "http://localhost:" + port);
        server.createContext("/", handler);
        server.start();
    }

    @Override
    public void stop() {
        server.stop(1);
    }

    @Override
    public void setLinksDao(Dao<String> dao) {
        if (started) {
            throw new IllegalStateException("Service already started");
        }
        this.dao = dao;
    }
}
