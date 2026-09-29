package company.vk.edu.distrib.compute.kl1dd.urlshortener;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.urlshortener.UrlShortenerService;

import java.io.IOException;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.NoSuchElementException;


public class MyUrlShortenerService implements UrlShortenerService {
    private final SecureRandom random = new SecureRandom();
    private final HttpServer httpServer;
    private final MyDao myDao;
    public MyUrlShortenerService(HttpServer httpServer) {
        this.httpServer = httpServer;
        this.myDao = new MyDao();

        httpServer.createContext("/v0/status", this::handleStatus);
        httpServer.createContext("/v0/links", this::handleLinks);
        httpServer.createContext("/", this::handleRedirect);
    }

    private String generateRandomID(int length) {
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            String alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
            int index = random.nextInt(alphabet.length());
            sb.append(alphabet.charAt(index));
        }
        return sb.toString();
    }


    private void handleStatus(HttpExchange exchange) throws IOException {
        if (!"GET".equals(exchange.getRequestMethod())) {
            exchange.sendResponseHeaders(405, -1);
            exchange.close();
            return;
        }

        exchange.sendResponseHeaders(200, -1);
        exchange.close();
    }

    private void handleLinks(HttpExchange exchange) throws IOException {
        String method = exchange.getRequestMethod();

        if (method.equals("POST")) {
            String linkBefore = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            if (!isValidURL(linkBefore)) {
                exchange.sendResponseHeaders(422, -1);
                exchange.close();
                return;
            }


            String id = generateRandomID(10);
            myDao.upsert(id, linkBefore);

            exchange.getResponseHeaders().set("Content-Type", "text/html; charset=utf-8");

            String linkAfter = "http://localhost:" + httpServer.getAddress().getPort() + "/" + id;
            byte[] responseBytes = linkAfter.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(201, responseBytes.length);
            exchange.getResponseBody().write(responseBytes);

            exchange.close();
        } else if (method.equals("GET")) {
            String reqPath = exchange.getRequestURI().getPath();
            String id = reqPath.substring("/v0/links/".length());

            if (!isValidID(id)) {
                exchange.sendResponseHeaders(422, -1);
                exchange.close();
                return;
            }

            try {
                exchange.getResponseHeaders().set("Content-Type", "text/html; charset=utf-8");

                String linkByID = myDao.get(id);
                byte[] responseBytes = linkByID.getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(200, responseBytes.length);
                exchange.getResponseBody().write(responseBytes);
                exchange.close();
            } catch (NoSuchElementException e) {
                exchange.sendResponseHeaders(404, -1);
                exchange.close();
            }
        } else if (method.equals("PUT")) {
            String reqPath = exchange.getRequestURI().getPath();
            String id = reqPath.substring("/v0/links/".length());
            if (!isValidID(id)) {
                exchange.sendResponseHeaders(422, -1);
                exchange.close();
                return;
            }

            try {
                myDao.get(id);


                String newLink = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
                if (!isValidURL(newLink)) {
                    exchange.sendResponseHeaders(422, -1);
                    exchange.close();
                    return;
                }

                myDao.upsert(id, newLink);

                exchange.sendResponseHeaders(200, -1);
                exchange.close();
            } catch (NoSuchElementException e) {
                exchange.sendResponseHeaders(404, -1);
                exchange.close();
            }
        } else if (method.equals("DELETE")) {
            String reqPath = exchange.getRequestURI().getPath();
            String id = reqPath.substring("/v0/links/".length());

            if (!isValidID(id)) {
                exchange.sendResponseHeaders(422, -1);
                exchange.close();
                return;
            }

            myDao.delete(id);
            exchange.sendResponseHeaders(202, -1);
            exchange.close();
        } else {
            exchange.sendResponseHeaders(405, -1);
            exchange.close();
        }
    }

    private void handleRedirect(HttpExchange exchange) throws IOException {
        String method = exchange.getRequestMethod();
        if (!method.equals("GET")) {
            exchange.sendResponseHeaders(405, -1);
            exchange.close();
            return;
        }

        String reqPath = exchange.getRequestURI().getPath();
        String id = reqPath.substring("/".length());
        if (!isValidID(id)) {
            exchange.sendResponseHeaders(422, -1);
            exchange.close();
            return;
        }

        try {
            String linkByID = myDao.get(id);

            exchange.getResponseHeaders().set("Location", linkByID);
            exchange.sendResponseHeaders(301, -1);
            exchange.close();
        } catch(NoSuchElementException e) {
            exchange.sendResponseHeaders(404, -1);
            exchange.close();
        }
    }

    private boolean isValidID(String id) {
        if (id == null || id.length() != 10) {
            return false;
        }

        for (int i = 0; i < 10; i++) {
            if (!(('A' <= id.charAt(i) && id.charAt(i) <= 'Z') || ('a' <= id.charAt(i) && id.charAt(i) <= 'z') ||
                    ('0' <= id.charAt(i) && id.charAt(i) <= '9'))) {
                return false;
            }
        }

        return true;
    }

    private boolean isValidURL(String url) {
        try {
            URI tryUri = URI.create(url);
            String connectionType = tryUri.getScheme(), site = tryUri.getHost();

            if (connectionType == null || site == null) {
                return false;
            }

            return connectionType.equals("http") || connectionType.equals("https");
        } catch(IllegalArgumentException e) {
            return false;
        }
    }


    @Override
    public void start() {
        httpServer.start();
    }

    @Override
    public void stop() {
        httpServer.stop(0);
    }
}
