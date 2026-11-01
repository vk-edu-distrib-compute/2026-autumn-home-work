package company.vk.edu.distrib.compute.annapiatkova.kv;

import company.vk.edu.distrib.compute.Dao;

import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpExchange;
import java.io.IOException;
import java.net.URI;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.NoSuchElementException;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.net.http.HttpResponse;

class ReplicatedKVServiceHandler implements HttpHandler {
    static final String STATUS = "/v0/status";
    static final String ENTITY_PREFIX = "/v0/entity?";
    static final String REPLICA_PREFIX = "/v0/replica?";
    static final String ID_PREFIX = "id=";
    static final String ACK_PREFIX = "ack=";
    static final String GET = "GET";
    static final String PUT = "PUT";
    static final String DELETE = "DELETE";
    static final int HTTP_OK = 200;
    static final int HTTP_CREATED = 201;
    static final int HTTP_ACCEPTED = 202;
    static final int HTTP_BAD_REQUEST = 400;
    static final int HTTP_NOT_FOUND = 404;
    static final int HTTP_SERVICE_UNAVAILABLE = 503;

    Dao<byte[]> dao;
    RequestCoordinator coordinator;
    int port;

    ReplicatedKVServiceHandler(Dao<byte[]> dao, int port) {
        this.dao = dao;
        this.coordinator = new RequestCoordinator();
        this.port = port;
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
            handleRemote(exchange, method, path);
        } else if (path.startsWith(REPLICA_PREFIX)) {
            handleLocal(exchange, method, path);
        }
        sendResponse(exchange, HTTP_NOT_FOUND);
    }

    private boolean validateRequest(String path) {
        String[] args = path.substring(ENTITY_PREFIX.length(), path.length()).split("&");
        if (args.length == 0 || !args[0].startsWith(ID_PREFIX) || args[0].length() == ID_PREFIX.length()) {
            return false;
        }
        if (args.length > 1 && args[1].startsWith(ACK_PREFIX)) {
            int ack = Integer.parseInt(args[1].substring(ACK_PREFIX.length(), args[1].length()));
            return coordinator.validateAck(ack);
        }
        return true;
    }

    private void handleRemote(HttpExchange exchange, String method, String path) throws IOException {
        if (!validateRequest(path)) {
            sendResponse(exchange, HTTP_BAD_REQUEST);
            return;            
        }
        String[] args = path.substring(ENTITY_PREFIX.length(), path.length()).split("&");
        String id = args[0].substring(ID_PREFIX.length(), args[0].length());
        int ack = 1;
        if (args.length > 1 && args[1].startsWith(ACK_PREFIX)) {
            ack = Integer.parseInt(args[1].substring(ACK_PREFIX.length(), args[1].length()));
        }
        switch (method) {
        case GET:
            handleGet(exchange, id, ack);
            return;
        case PUT:
            handlePut(exchange, id, ack);
            return;
        case DELETE:
            handleDelete(exchange, id, ack);
            return;
        default:
            break;
        }
    }

    private void handleLocal(HttpExchange exchange, String method, String path) throws IOException {
        String id = path.substring(REPLICA_PREFIX.length(), path.length());
        if (id.length() == 0) {
            sendResponse(exchange, HTTP_BAD_REQUEST);
            return;
        }
        switch (method) {
        case GET:
            handleGetLocal(exchange, id);
            return;
        case PUT:
            handlePutLocal(exchange, id);
            return;
        case DELETE:
            handleDeleteLocal(exchange, id);
            return;
        default:
            break;
        }
    }

    private void handleGetLocal(HttpExchange exchange, String id) throws IOException {
        try {
            byte[] data = dao.get(id);
            sendResponse(exchange, HTTP_OK, data);
        } catch (NoSuchElementException e) {
            sendResponse(exchange, HTTP_NOT_FOUND);
        }
    }

    private void handlePutLocal(HttpExchange exchange, String id) throws IOException {
        try (InputStream is = exchange.getRequestBody()) {
            byte[] data = is.readAllBytes();
            dao.upsert(id, data);
        }
        sendResponse(exchange, HTTP_CREATED);
    }

    private void handleDeleteLocal(HttpExchange exchange, String id) throws IOException {
        dao.delete(id);
        sendResponse(exchange, HTTP_ACCEPTED);
    }

    private void handleGet(HttpExchange exchange, String id, int ack) throws IOException {
        int repliesCount = 0;
        byte[] value;
        List<Integer> ports = coordinator.getReplicaPorts(id);
        List<CompletableFuture<HttpResponse<byte[]>>> responses = new ArrayList<>(ports.size());
        for (int port : ports) {
            if (port == this.port) {
                try {
                    dao.get(id); 
                    repliesCount += 1;
                } catch (NoSuchElementException e) {
                    repliesCount += 1;
                } catch (IOException e) {
                    continue;
                }
                continue;
            }
            responses.add(coordinator.sendGet(port, id));
        }
        RequestCoordinator.Result res = RequestCoordinator.handleGetResponses(responses, repliesCount);
        repliesCount = res.acks;
        value = res.value;
        if (repliesCount < ack) {
            sendResponse(exchange, HTTP_SERVICE_UNAVAILABLE);
            return;
        }
        if (value != null) {
            sendResponse(exchange, HTTP_OK, value);
        } else {
            sendResponse(exchange, HTTP_NOT_FOUND);
        }
    }

    private void handlePut(HttpExchange exchange, String id, int ack) throws IOException {
        try (InputStream is = exchange.getRequestBody()) {
            byte[] data = is.readAllBytes();
            int repliesCount = 0;
            List<Integer> ports = coordinator.getReplicaPorts(id);
            List<CompletableFuture<HttpResponse<byte[]>>> responses = new ArrayList<>(ports.size());
            for (int port : ports) {
                if (port == this.port) {
                    try {
                        dao.upsert(id, data);
                        repliesCount += 1;
                    } catch (IOException e) {
                        continue;
                    }
                    continue;
                }
                responses.add(coordinator.sendPut(port, id, data));
            }
            repliesCount += RequestCoordinator.handlePutResponses(responses);
            if (repliesCount < ack) {
                sendResponse(exchange, HTTP_SERVICE_UNAVAILABLE);
                return;
            }
        }
        sendResponse(exchange, HTTP_CREATED);
    }

    private void handleDelete(HttpExchange exchange, String id, int ack) throws IOException {
        int repliesCount = 0;
        List<Integer> ports = coordinator.getReplicaPorts(id);
        List<CompletableFuture<HttpResponse<byte[]>>> responses = new ArrayList<>(ports.size());
        for (int port : ports) {
            if (port == this.port) {
                try {
                    dao.delete(id);
                    repliesCount += 1;
                } catch (IOException e) {
                    continue;
                }
                continue;
            }
            responses.add(coordinator.sendDelete(port, id));
        }
        repliesCount += RequestCoordinator.handleDeleteResponses(responses);
        if (repliesCount < ack) {
            sendResponse(exchange, HTTP_SERVICE_UNAVAILABLE);
            return;
        }
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
