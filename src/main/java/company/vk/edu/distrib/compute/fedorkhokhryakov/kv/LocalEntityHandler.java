package company.vk.edu.distrib.compute.fedorkhokhryakov.kv;

import com.sun.net.httpserver.HttpExchange;
import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.util.NoSuchElementException;

public class LocalEntityHandler implements EntityHandler {
    private static final int OK = 200;
    private static final int CREATED = 201;
    private static final int ACCEPTED = 202;
    private static final int NOT_FOUND = 404;
    private static final int METHOD_NOT_ALLOWED = 405;
    private static final int NO_BODY = -1;

    private final Dao<byte[]> dao;

    public LocalEntityHandler(Dao<byte[]> dao) {
        this.dao = dao;
    }

    @Override
    public void handle(HttpExchange exchange, String id) throws IOException {
        switch (exchange.getRequestMethod()) {
            case "GET" -> {
                try {
                    sendBody(exchange, OK, dao.get(id));
                } catch (NoSuchElementException e) {
                    exchange.sendResponseHeaders(NOT_FOUND, NO_BODY);
                }
            }
            case "PUT" -> {
                dao.upsert(id, exchange.getRequestBody().readAllBytes());
                exchange.sendResponseHeaders(CREATED, NO_BODY);
            }
            case "DELETE" -> {
                dao.delete(id);
                exchange.sendResponseHeaders(ACCEPTED, NO_BODY);
            }
            default -> exchange.sendResponseHeaders(METHOD_NOT_ALLOWED, NO_BODY);
        }
    }

    @Override
    public void close() throws IOException {
        dao.close();
    }

    static void sendBody(HttpExchange exchange, int status, byte[] body) throws IOException {
        exchange.sendResponseHeaders(status, body.length == 0 ? NO_BODY : body.length);
        exchange.getResponseBody().write(body);
    }
}
