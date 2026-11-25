package company.vk.edu.distrib.compute.kl1dd.kv;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.kv.KVService;

import java.io.IOException;

public class MyKVService implements KVService {
    private static final String GET_METHOD = "GET";
    private static final int STOP_DELAY_SECONDS = 2;
    private static final int MAX_REQUEST_BODY_SIZE = 5 * 1024 * 1024;
    private static final String CLUSTER_PROPERTY = "CLUSTER";
    private static final String INTERNAL_REQUEST_HEADER = "Internal-Request";
    private static final String INTERNAL_REQUEST_VALUE = "true";

    private final HttpServer httpServer;
    private final ReplicationImpl replication = new ReplicationImpl();
    private final ReplicationClient replicationClient = new ReplicationClient();
    private final EntityHandler entityHandler;

    private int[] getClusterPorts() {
        String cluster = System.getProperty(CLUSTER_PROPERTY);
        if (cluster == null || cluster.isEmpty()) {
            return new int[]{httpServer.getAddress().getPort()};
        }

        String[] ports = cluster.split(",");
        int[] results = new int[ports.length];
        for (int i = 0; i < ports.length; i++) {
            results[i] = Integer.parseInt(ports[i]);
        }
        return results;
    }

    private String getQueryParameter(HttpExchange exchange, String name) {
        String query = exchange.getRequestURI().getQuery();
        if (query == null) {
            return null;
        }

        String[] splitted = query.split("&");
        String prefix = name + "=";
        for (String param : splitted) {
            if (param.startsWith(prefix)) {
                return param.substring(prefix.length());
            }
        }

        return null;
    }

    private boolean isInternalRequest(HttpExchange exchange) {
        String value = exchange.getRequestHeaders().getFirst(INTERNAL_REQUEST_HEADER);
        return INTERNAL_REQUEST_VALUE.equals(value);
    }

    public MyKVService(HttpServer httpServer) throws IOException {
        this.httpServer = httpServer;
        DaoHelper daoHelper = new DaoHelper(httpServer.getAddress().getPort());
        this.entityHandler = new EntityHandler(daoHelper, replication, replicationClient,
                                               httpServer.getAddress().getPort());

        httpServer.createContext("/v0/status", this::handleStatus);
        httpServer.createContext("/v0/entity", this::handleEntity);
    }

    private void handleStatus(HttpExchange exchange) throws IOException {
        if (!GET_METHOD.equals(exchange.getRequestMethod())) {
            exchange.sendResponseHeaders(405, -1);
            exchange.close();
            return;
        }

        exchange.sendResponseHeaders(200, -1);
        exchange.close();
    }

    private void handleEntity(HttpExchange exchange) throws IOException {
        String method = exchange.getRequestMethod();
        switch (method) {
            case "GET" -> {
                handleGet(exchange);
            }
            case "PUT" -> {
                handlePut(exchange);
            }
            case "DELETE" -> {
                handleDelete(exchange);
            }
            default -> {
                exchange.sendResponseHeaders(405, -1);
                exchange.close();
            }
        }
    }

    // DELETE
    private void handleDelete(HttpExchange exchange) throws IOException {
        String id = getId(exchange);
        if (id == null) {
            exchange.sendResponseHeaders(400, -1);
            exchange.close();
            return;
        }

        if (isInternalRequest(exchange)) {
            entityHandler.handleLocalDelete(exchange, id);
            return;
        }

        int ack = getAck(exchange);
        boolean ackSpecified = (getQueryParameter(exchange, "ack") != null);
        if (ack == -1) {
            exchange.sendResponseHeaders(400, -1);
            exchange.close();
            return;
        }

        //handledelete
        entityHandler.handleDelete(exchange, id, ack, ackSpecified, getClusterPorts());
    }

    // PUT
    private void handlePut(HttpExchange exchange) throws IOException {
        String id = getId(exchange);
        if (id == null) {
            exchange.sendResponseHeaders(400, -1);
            exchange.close();
            return;
        }

        byte[] value;
        try {
            value = exchange.getRequestBody().readNBytes(MAX_REQUEST_BODY_SIZE + 1);
            if (value.length > MAX_REQUEST_BODY_SIZE) {
                exchange.sendResponseHeaders(413, -1);
                exchange.close();
                return;
            }
        } catch (IOException e) {
            exchange.sendResponseHeaders(503, -1);
            exchange.close();
            return;
        }

        if (isInternalRequest(exchange)) {
            entityHandler.handleLocalPut(exchange, id, value);
            return;
        }

        int ack = getAck(exchange);
        if (ack == -1) {
            exchange.sendResponseHeaders(400, -1);
            exchange.close();
            return;
        }

        entityHandler.handlePut(exchange, id, value, ack, getClusterPorts());
    }

    // GET
    private void handleGet(HttpExchange exchange) throws IOException {
        String id = getId(exchange);
        if (id == null) {
            exchange.sendResponseHeaders(400, -1);
            exchange.close();
            return;
        }

        if (isInternalRequest(exchange)) {
            entityHandler.handleLocalGet(exchange, id);
            return;
        }

        int ack = getAck(exchange);
        if (ack == -1) {
            exchange.sendResponseHeaders(400, -1);
            exchange.close();
            return;
        }

        entityHandler.handleGet(exchange, id, ack, getClusterPorts());
    }

    private String getId(HttpExchange exchange) {
        String id = getQueryParameter(exchange, "id");
        if (id == null || id.isEmpty()) {
            return null;
        }

        return id;
    }

    private int getAck(HttpExchange exchange) {
        String ack = getQueryParameter(exchange, "ack");
        if (ack == null) {
            return 1;
        }

        int ackInt;
        try {
            ackInt = Integer.parseInt(ack);
        } catch (NumberFormatException e) {
            return -1;
        }

        if (ackInt < 1 || ackInt > getClusterPorts().length) {
            return -1;
        }
        return ackInt;
    }

    @Override
    public void start() {
        httpServer.start();
    }

    @Override
    public void stop() {
        httpServer.stop(STOP_DELAY_SECONDS);
    }
}
