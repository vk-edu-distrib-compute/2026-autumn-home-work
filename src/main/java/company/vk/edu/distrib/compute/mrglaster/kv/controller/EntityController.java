package company.vk.edu.distrib.compute.mrglaster.kv.controller;

import com.sun.net.httpserver.HttpExchange;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.mrglaster.kv.replication.replica.ReplicaResult;
import company.vk.edu.distrib.compute.mrglaster.kv.replication.coordination.ReplicationCoordinator;
import company.vk.edu.distrib.compute.mrglaster.urlshortener.annotation.Route;
import company.vk.edu.distrib.compute.mrglaster.urlshortener.controller.enums.StatusCode;
import company.vk.edu.distrib.compute.mrglaster.urlshortener.controller.network.NetworkInteractable;

import java.io.IOException;
import java.util.Map;
import java.util.NoSuchElementException;

public class EntityController implements NetworkInteractable {

    private final Dao<byte[]> localDao;
    private final ReplicationCoordinator coordinator;
    private static final String INTERNAL_HEADER_KEY = "X-Internal-Request";
    private static final String INTERNAL_HEADER_VALUE = "true";
    private static final String ACK_PARAM = "ack";
    private static final int DEFAULT_ACK = 1;

    public EntityController(Dao<byte[]> localDao, ReplicationCoordinator coordinator) {
        this.localDao = localDao;
        this.coordinator = coordinator;
    }

    @Route(method = "GET", path = "/v0/entity", requiresAuthorization = false)
    public void getEntity(HttpExchange exchange, Map<String, String> pathParams) throws IOException {
        String id = pathParams.get("id");
        if (isInvalidId(id)) {
            sendStatusCodeResponse(exchange, StatusCode.HTTP_BAD_REQUEST);
            return;
        }
        if (isInternal(exchange)) {
            handleLocalGet(exchange, id);
            return;
        }
        Integer ack = parseAckOrFail(exchange, pathParams);
        if (ack == null) {
            return;
        }
        ReplicaResult result = coordinator.execute("GET", id, new byte[0], ack);
        respondWithResult(exchange, result);
    }

    @Route(method = "PUT", path = "/v0/entity", requiresAuthorization = false)
    public void upsertEntity(HttpExchange exchange, Map<String, String> pathParams) throws IOException {
        String id = pathParams.get("id");
        if (isInvalidId(id)) {
            sendStatusCodeResponse(exchange, StatusCode.HTTP_BAD_REQUEST);
            return;
        }
        byte[] body = exchange.getRequestBody().readAllBytes();
        if (isInternal(exchange)) {
            handleLocalPut(exchange, id, body);
            return;
        }
        Integer ack = parseAckOrFail(exchange, pathParams);
        if (ack == null) {
            return;
        }
        ReplicaResult result = coordinator.execute("PUT", id, body, ack);
        respondWithResult(exchange, result);
    }

    @Route(method = "DELETE", path = "/v0/entity", requiresAuthorization = false)
    public void deleteEntity(HttpExchange exchange, Map<String, String> pathParams) throws IOException {
        String id = pathParams.get("id");
        if (isInvalidId(id)) {
            sendStatusCodeResponse(exchange, StatusCode.HTTP_BAD_REQUEST);
            return;
        }
        if (isInternal(exchange)) {
            handleLocalDelete(exchange, id);
            return;
        }
        Integer ack = parseAckOrFail(exchange, pathParams);
        if (ack == null) {
            return;
        }
        ReplicaResult result = coordinator.execute("DELETE", id, new byte[0], ack);
        respondWithResult(exchange, result);
    }

    private void handleLocalGet(HttpExchange exchange, String id) throws IOException {
        try {
            byte[] value = localDao.get(id);
            sendBytesResponse(exchange, value, StatusCode.HTTP_OK);
        } catch (NoSuchElementException e) {
            sendStatusCodeResponse(exchange, StatusCode.HTTP_NOT_FOUND);
        } catch (IllegalArgumentException | IOException e) {
            sendStatusCodeResponse(exchange, StatusCode.HTTP_INTERNAL_SERVER_ERROR);
        }
    }

    private void handleLocalPut(HttpExchange exchange, String id, byte[] body) throws IOException {
        try {
            localDao.upsert(id, body);
            sendStatusCodeResponse(exchange, StatusCode.HTTP_CREATED);
        } catch (IllegalArgumentException | IOException e) {
            sendStatusCodeResponse(exchange, StatusCode.HTTP_INTERNAL_SERVER_ERROR);
        }
    }

    private void handleLocalDelete(HttpExchange exchange, String id) throws IOException {
        try {
            localDao.delete(id);
            sendStatusCodeResponse(exchange, StatusCode.HTTP_ACCEPTED);
        } catch (IllegalArgumentException | IOException e) {
            sendStatusCodeResponse(exchange, StatusCode.HTTP_INTERNAL_SERVER_ERROR);
        }
    }

    private Integer parseAckOrFail(HttpExchange exchange, Map<String, String> params) throws IOException {
        String raw = params.get(ACK_PARAM);
        int n = coordinator.replicationFactor();
        int ack;
        try {
            ack = raw == null ? DEFAULT_ACK : Integer.parseInt(raw);
        } catch (NumberFormatException e) {
            sendStatusCodeResponse(exchange, StatusCode.HTTP_BAD_REQUEST);
            return null;
        }
        if (ack < 1 || ack > n) {
            sendStatusCodeResponse(exchange, StatusCode.HTTP_BAD_REQUEST);
            return null;
        }
        return ack;
    }

    private void respondWithResult(HttpExchange exchange, ReplicaResult result) throws IOException {
        if (result.body() != null && result.body().length > 0) {
            sendBytesResponse(exchange, result.body(), statusFromCode(result.status()));
        } else {
            sendStatusCodeResponse(exchange, statusFromCode(result.status()));
        }
    }

    private static StatusCode statusFromCode(int code) {
        return switch (code) {
            case 200 -> StatusCode.HTTP_OK;
            case 201 -> StatusCode.HTTP_CREATED;
            case 202 -> StatusCode.HTTP_ACCEPTED;
            case 400 -> StatusCode.HTTP_BAD_REQUEST;
            case 404 -> StatusCode.HTTP_NOT_FOUND;
            case 503 -> StatusCode.HTTP_SERVICE_UNAVAILABLE;
            default -> StatusCode.HTTP_INTERNAL_SERVER_ERROR;
        };
    }

    private boolean isInvalidId(String id) {
        return id == null || id.isEmpty();
    }

    private boolean isInternal(HttpExchange exchange) {
        return INTERNAL_HEADER_VALUE.equals(exchange.getRequestHeaders().getFirst(INTERNAL_HEADER_KEY));
    }
}
