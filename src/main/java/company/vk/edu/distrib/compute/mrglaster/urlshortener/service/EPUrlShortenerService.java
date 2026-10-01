package company.vk.edu.distrib.compute.mrglaster.urlshortener.service;

import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.mrglaster.urlshortener.controller.route.external.LinkController;
import company.vk.edu.distrib.compute.mrglaster.urlshortener.controller.route.external.StatusController;
import company.vk.edu.distrib.compute.mrglaster.urlshortener.controller.route.internal.UserController;
import company.vk.edu.distrib.compute.mrglaster.urlshortener.dao.PersistentDao;
import company.vk.edu.distrib.compute.mrglaster.urlshortener.controller.manager.ControllerManager;
import company.vk.edu.distrib.compute.mrglaster.urlshortener.exception.DataSavingException;

import java.io.IOException;
import java.net.InetSocketAddress;

public class EPUrlShortenerService implements company.vk.edu.distrib.compute.urlshortener.UrlShortenerService {

    private static final int SHUTDOWN_DELAY = 5;
    private static final String USERS_STORAGE_FILE = "/tmp/users";
    private static final String URL_STORAGE_FILE = "/tmp/urls";

    private final HttpServer httpServer;
    private final PersistentDao userDao;
    private final PersistentDao urlDao;

    public EPUrlShortenerService(int port) throws IOException {
        this.httpServer = HttpServer.create(new InetSocketAddress(port), 0);

        final String baseUrl = "http://127.0.0.1:" + port;

        this.urlDao = new PersistentDao(URL_STORAGE_FILE);
        this.userDao = new PersistentDao(USERS_STORAGE_FILE);

        AuthorizationService authService = new AuthorizationService(userDao);

        ControllerManager routingManager = new ControllerManager(authService);
        routingManager.addController(new StatusController());
        routingManager.addController(new LinkController(urlDao, baseUrl));
        routingManager.addController(new UserController(userDao));
        routingManager.register(httpServer);
    }

    @Override
    public void start() {
        httpServer.start();
    }

    @Override
    public void stop() {
        try {
            userDao.save();
            urlDao.save();
        } catch (Exception e) {
            throw new DataSavingException("Unable to save data to files", e);
        } finally {
            httpServer.stop(SHUTDOWN_DELAY);
        }
    }
}
