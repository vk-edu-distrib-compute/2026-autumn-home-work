package company.vk.edu.distrib.compute.annapiatkova.kv;

import company.vk.edu.distrib.compute.Dao;

import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpExchange;
import java.io.IOException;
import java.net.URI;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.NoSuchElementException;

class KVServiceHandler implements HttpHandler {
    static final String STATUS = "/v0/status";
    static final String ENTITY_PREFIX = "/v0/entity?id=";
    static final String GET = "GET";
    static final String PUT = "PUT";
    static final String DELETE = "DELETE";
    static final int HTTP_OK = 200;
    static final int HTTP_CREATED = 201;
    static final int HTTP_ACCEPTED = 202;
    static final int HTTP_BAD_REQUEST = 400;
    static final int HTTP_NOT_FOUND = 404;

    Dao<byte[]> dao;

    KVServiceHandler(Dao<byte[]> dao) {
        this.dao = dao;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        String method = exchange.getRequestMethod();
        URI uri = exchange.getRequestURI();
        String path = uri.toString();
        if (STATUS.equals(path) && GET.equals(method)) {
            sendResponse(exchange, HTTP_OK);
            return;
        }
        if (path.startsWith(ENTITY_PREFIX)) {
            String id = path.substring(ENTITY_PREFIX.length(), path.length());
            if (id.length() == 0) {
                sendResponse(exchange, HTTP_BAD_REQUEST);
                return;
            }
            switch (method) {
            case GET:
                handleGet(exchange, id);
                return;
            case PUT:
                handlePut(exchange, id);
                return;
            case DELETE:
                handleDelete(exchange, id);
                return;
            default:
                break;
            }
        }
        sendResponse(exchange, HTTP_NOT_FOUND);
    }

    private void handleGet(HttpExchange exchange, String id) throws IOException {
        try {
            byte[] data = dao.get(id);
            sendResponse(exchange, HTTP_OK, data);
        } catch (NoSuchElementException e) {
            sendResponse(exchange, HTTP_NOT_FOUND);
        }
    }

    private void handlePut(HttpExchange exchange, String id) throws IOException {
        try (InputStream is = exchange.getRequestBody()) {
            byte[] data = is.readAllBytes();
            dao.upsert(id, data);
        }
        sendResponse(exchange, HTTP_CREATED);
    }

    private void handleDelete(HttpExchange exchange, String id) throws IOException {
        dao.delete(id);
        sendResponse(exchange, HTTP_ACCEPTED);
    }

    private void sendResponse(HttpExchange exchange, int code) throws IOException {
        exchange.sendResponseHeaders(code, 0);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write("".getBytes());
        }
    }

    private void sendResponse(HttpExchange exchange, int code, byte[] content) throws IOException {
        exchange.getResponseHeaders().add("Content-Type", "application/octet-stream");
        exchange.sendResponseHeaders(code, content.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(content);
        }
    }
}
