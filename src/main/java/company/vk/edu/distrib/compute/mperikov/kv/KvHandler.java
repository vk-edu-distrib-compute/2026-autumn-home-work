package company.vk.edu.distrib.compute.mperikov.kv;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.http.HttpClient;
import java.util.NoSuchElementException;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import company.vk.edu.distrib.compute.Dao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class KvHandler implements HttpHandler {
    private static final Logger log = LoggerFactory.getLogger(KvHandler.class);

    private final Dao<byte[]> entities;
    private final KvCoordinator coordinator;

    KvHandler(int port, Dao<byte[]> entities, HttpClient client) {
        this.entities = entities;
        coordinator = new KvCoordinator(port, entities, client);
    }

    @Override
    public void handle(HttpExchange exchange) {
        try (exchange) {
            try {
                route(exchange);
            } catch (Exception ex) {
                if (log.isWarnEnabled()) {
                    log.warn("KV request failed", ex);
                }
                sendEmpty(exchange, KvCodes.SERVER_ERROR);
            }
        }
    }

    private void route(HttpExchange exchange) throws IOException {
        String method = exchange.getRequestMethod();
        String path = exchange.getRequestURI().getPath();
        if (KvCodes.isStatus(method, path)) {
            sendEmpty(exchange, KvCodes.OK);
            return;
        }
        if (KvCodes.isEntity(path)) {
            serveEntity(exchange, method, path);
            return;
        }
        sendEmpty(exchange, KvCodes.NOT_FOUND);
    }

    private void serveEntity(HttpExchange exchange, String method, String path) throws IOException {
        String query = exchange.getRequestURI().getRawQuery();
        String id = KvCodes.pathId(path);
        if (id.isEmpty()) {
            id = KvCodes.entityId(query);
        }
        if (id.isEmpty()) {
            sendEmpty(exchange, KvCodes.BAD_REQUEST);
            return;
        }
        if (KvCodes.storedLocally(query)) {
            apply(exchange, method, id);
            return;
        }
        int[] members = KvCodes.memberPorts();
        int ack = KvCodes.requiredAck(query, KvCodes.replicaTotal(members));
        if (ack < 0) {
            sendEmpty(exchange, KvCodes.BAD_REQUEST);
            return;
        }
        if (members.length == 0) {
            apply(exchange, method, id);
            return;
        }
        byte[] body = KvCodes.PUT.equals(method) ? readBody(exchange) : new byte[0];
        coordinator.serve(exchange, method, id, ack, body);
    }

    private void apply(HttpExchange exchange, String method, String id) throws IOException {
        if (KvCodes.GET.equals(method)) {
            readEntity(exchange, id);
            return;
        }
        if (KvCodes.PUT.equals(method)) {
            writeEntity(exchange, id);
            return;
        }
        if (KvCodes.DELETE.equals(method)) {
            deleteEntity(exchange, id);
            return;
        }
        sendEmpty(exchange, KvCodes.NOT_FOUND);
    }

    private void readEntity(HttpExchange exchange, String id) throws IOException {
        try {
            sendBytes(exchange, KvCodes.OK, entities.get(id));
        } catch (NoSuchElementException expected) {
            sendEmpty(exchange, KvCodes.NOT_FOUND);
        }
    }

    private void writeEntity(HttpExchange exchange, String id) throws IOException {
        entities.upsert(id, readBody(exchange));
        sendEmpty(exchange, KvCodes.CREATED);
    }

    private void deleteEntity(HttpExchange exchange, String id) throws IOException {
        entities.delete(id);
        sendEmpty(exchange, KvCodes.ACCEPTED);
    }

    private static byte[] readBody(HttpExchange exchange) throws IOException {
        try (InputStream input = exchange.getRequestBody()) {
            return input.readAllBytes();
        }
    }

    static void sendBytes(HttpExchange exchange, int status, byte[] body) throws IOException {
        exchange.sendResponseHeaders(status, body.length);
        if (body.length == 0) {
            return;
        }
        try (OutputStream output = exchange.getResponseBody()) {
            output.write(body);
        }
    }

    static void sendEmpty(HttpExchange exchange, int status) {
        try {
            exchange.sendResponseHeaders(status, -1);
        } catch (IOException ex) {
            if (log.isDebugEnabled()) {
                log.debug("KV response was not sent", ex);
            }
        }
    }
}
