package company.vk.edu.distrib.compute.flighen.urlshortener;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import company.vk.edu.distrib.compute.Dao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.io.IOException;

public class LinkHandler implements HttpHandler {
    private static final Logger log =
            LoggerFactory.getLogger(LinkHandler.class);

    private static final int ID_LENGTH = 10;

    private static final String ENCODING = "utf-8";
    private static final String MEDIA_TYPE = "text/html";
    private static final String CHARSET = "charset";

    private final Dao<String> dao;

    private final int port;

    public LinkHandler(Dao<String> dao, int port) {
        this.dao = dao;
        this.port = port;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        final var method = exchange.getRequestMethod();

        try (exchange) {
            switch (method) {
                case "GET":
                    handeGet(exchange);
                    break;
                case "POST":
                    handlePost(exchange);
                    break;
                case "PUT":
                    handlePut(exchange);
                    break;
                case "DELETE":
                    handleDelete(exchange);
                    break;
                default:
                    exchange.sendResponseHeaders(403, -1);
                    break;
            }
        } catch (IOException e) {
            if (log.isErrorEnabled()) {
                log.error(
                        "I/O error while handling {} {}",
                        exchange.getRequestMethod(),
                        exchange.getRequestURI(),
                        e
                );
            }
            throw e;
        }

        exchange.close();
    }

    private void handlePut(HttpExchange exchange) throws IOException {
        String[] path = exchange.getRequestURI().getPath().split("/");

        String id = path[path.length - 1];

        if (id == null || !IdUtils.isValidId(id)) {
            exchange.sendResponseHeaders(422, -1);
            return;
        }

        if (!validateHeaders(exchange)) {
            exchange.sendResponseHeaders(415, -1);
            return;
        }

        if (!dao.exists(id)) {
            exchange.sendResponseHeaders(404, -1);
        }

        String newLongLink;

        try (InputStream input = exchange.getRequestBody()) {
            newLongLink = new String(input.readAllBytes(), StandardCharsets.UTF_8);
        }

        if (!(newLongLink.contains("https") || newLongLink.contains("http"))) {
            exchange.sendResponseHeaders(422, -1);
            return;
        }

        try {
            dao.upsert(id, newLongLink);

            exchange.sendResponseHeaders(200, -1);
        } catch (IllegalArgumentException e) {
            exchange.sendResponseHeaders(422, -1);
        }
    }

    private void handleDelete(HttpExchange exchange) throws IOException {
        String[] path = exchange.getRequestURI().getPath().split("/");

        String id = path[path.length - 1];

        if (id == null || !IdUtils.isValidId(id)) {
            exchange.sendResponseHeaders(422, -1);
            return;
        }

        try {
            dao.delete(id);

            exchange.sendResponseHeaders(202, -1);
        } catch (IllegalArgumentException e) {
            exchange.sendResponseHeaders(422, -1);
        }
    }

    private void handeGet(HttpExchange exchange) throws IOException {
        String[] path = exchange.getRequestURI().getPath().split("/");

        String id = path[path.length - 1];

        if (id == null || !IdUtils.isValidId(id)) {
            exchange.sendResponseHeaders(422, -1);
            return;
        }

        try {
            String link = dao.get(id);

            exchange.getResponseHeaders()
                    .add("Content-Type", "%s; charset=%s".formatted(MEDIA_TYPE, ENCODING));

            exchange.sendResponseHeaders(200, link.length());

            exchange.getResponseBody().write(link.getBytes(StandardCharsets.UTF_8));
        } catch (IllegalArgumentException e) {
            exchange.sendResponseHeaders(422, -1);
        } catch (NoSuchElementException e) {
            exchange.sendResponseHeaders(404, -1);
        }
    }

    private void handlePost(HttpExchange exchange) throws IOException {
        if (!validateHeaders(exchange)) {
            exchange.sendResponseHeaders(415, -1);
            return;
        }

        String longUrl;

        try (InputStream input = exchange.getRequestBody()) {
            longUrl = new String(input.readAllBytes(), StandardCharsets.UTF_8);
        }

        if (!(longUrl.contains("https") || longUrl.contains("http"))) {
            exchange.sendResponseHeaders(422, -1);
            return;
        }

        String generatedID = IdUtils.getId(ID_LENGTH);

        try {
            dao.upsert(generatedID, longUrl);
        } catch (IllegalArgumentException e) {
            exchange.sendResponseHeaders(422, -1);
            return;
        }

        exchange.getResponseHeaders()
                .add("Content-Type", "%s; charset=%s".formatted(MEDIA_TYPE, ENCODING));

        String responseUrl = "http://localhost:" + port + "/" + generatedID;

        exchange.sendResponseHeaders(201, responseUrl.length());
        exchange.getResponseBody().write(responseUrl.getBytes(StandardCharsets.UTF_8));
    }

    private boolean validateHeaders(HttpExchange exchange) throws IOException {
        String rawContentType = exchange.getRequestHeaders().getFirst("Content-Type");

        if (rawContentType == null) {
            return false;
        }

        String[] parts = rawContentType.split(";");

        String mediaType = parts[0].trim();

        if (!MEDIA_TYPE.equalsIgnoreCase(mediaType)) {
            return false;
        }

        String charset = "";

        for (int i = 1; i < parts.length; i++) {
            String[] parameter = parts[i].trim().split("=", 2);

            if (parameter.length == 2 && CHARSET.equalsIgnoreCase(parameter[0].trim())) {
                charset = parameter[1].trim().replace("\"", "");
            }
        }

        return ENCODING.equalsIgnoreCase(charset);
    }
}
