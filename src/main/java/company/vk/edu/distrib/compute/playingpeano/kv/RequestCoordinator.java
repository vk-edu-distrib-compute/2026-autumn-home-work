package company.vk.edu.distrib.compute.playingpeano.kv;

import com.sun.net.httpserver.HttpExchange;

import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;

import org.jspecify.annotations.Nullable;

final class RequestCoordinator {
    private static final String GET_METHOD = "GET";
    private static final String PUT_METHOD = "PUT";
    private static final String DELETE_METHOD = "DELETE";
    private static final String ALLOWED_ENTITY_METHODS = "GET, PUT, DELETE";
    private static final byte[] EMPTY_VALUE = new byte[0];

    private final int localPort;
    private final PersistentByteArrayDao dao;
    private final ReplicaClient replicaClient;

    RequestCoordinator(int localPort, PersistentByteArrayDao dao, ReplicaClient replicaClient) {
        this.localPort = localPort;
        this.dao = dao;
        this.replicaClient = replicaClient;
    }

    void handle(HttpExchange exchange, EntityQuery query) throws IOException {
        if (isReplicaRequest(exchange) || !ClusterTopology.isConfigured()) {
            serveLocal(exchange, query.key());
            return;
        }
        coordinate(exchange, query);
    }

    private void coordinate(HttpExchange exchange, EntityQuery query) throws IOException {
        ClusterTopology topology = ClusterTopology.current(localPort);
        int ack = query.ack(topology.size());
        switch (exchange.getRequestMethod()) {
            case GET_METHOD -> coordinateGet(exchange, query.key(), ack, topology);
            case PUT_METHOD -> coordinatePut(exchange, query.key(), ack, topology);
            case DELETE_METHOD -> coordinateDelete(exchange, query.key(), ack, topology);
            default -> HttpResponses.sendMethodNotAllowed(exchange, ALLOWED_ENTITY_METHODS);
        }
    }

    private void serveLocal(HttpExchange exchange, String key) throws IOException {
        switch (exchange.getRequestMethod()) {
            case GET_METHOD -> HttpResponses.sendValue(exchange, dao.get(key));
            case PUT_METHOD -> {
                dao.upsert(key, exchange.getRequestBody().readAllBytes());
                HttpResponses.sendEmpty(exchange, HttpResponses.STATUS_CREATED);
            }
            case DELETE_METHOD -> {
                dao.delete(key);
                HttpResponses.sendEmpty(exchange, HttpResponses.STATUS_ACCEPTED);
            }
            default -> HttpResponses.sendMethodNotAllowed(exchange, ALLOWED_ENTITY_METHODS);
        }
    }

    private void coordinateGet(
        HttpExchange exchange,
        String key,
        int ack,
        ClusterTopology topology
    ) throws IOException {
        int[] replicas = topology.replicasFor(key);
        GetAckLatch latch = new GetAckLatch(ack, replicas.length);
        for (int replica : replicas) {
            getOne(key, replica).whenComplete((result, error) -> latch.record(resultOrFailure(result)));
        }
        switch (latch.await()) {
            case FOUND -> HttpResponses.sendValue(exchange, latch.value());
            case MISSING -> HttpResponses.sendEmpty(exchange, HttpResponses.STATUS_NOT_FOUND);
            case UNAVAILABLE -> HttpResponses.sendEmpty(exchange, HttpResponses.STATUS_UNAVAILABLE);
        }
    }

    private void coordinatePut(
        HttpExchange exchange,
        String key,
        int ack,
        ClusterTopology topology
    ) throws IOException {
        byte[] value = exchange.getRequestBody().readAllBytes();
        int confirmed = countWriteAcks(key, topology, value, false, ack);
        HttpResponses.sendEmpty(
            exchange,
            confirmed >= ack ? HttpResponses.STATUS_CREATED : HttpResponses.STATUS_UNAVAILABLE
        );
    }

    private void coordinateDelete(
        HttpExchange exchange,
        String key,
        int ack,
        ClusterTopology topology
    ) throws IOException {
        int confirmed = countWriteAcks(key, topology, EMPTY_VALUE, true, ack);
        HttpResponses.sendEmpty(
            exchange,
            confirmed >= ack ? HttpResponses.STATUS_OK : HttpResponses.STATUS_UNAVAILABLE
        );
    }

    private CompletableFuture<ReplicaClient.GetResult> getOne(String key, int replica) {
        if (replica == localPort) {
            return CompletableFuture.supplyAsync(() -> localGet(key));
        }
        return replicaClient.getAsync(replica, key);
    }

    private int countWriteAcks(
        String key,
        ClusterTopology topology,
        byte[] value,
        boolean delete,
        int ack
    ) {
        int[] replicas = topology.replicasFor(key);
        WriteAckLatch latch = new WriteAckLatch(ack, replicas.length);
        for (int replica : replicas) {
            writeOne(key, replica, value, delete).whenComplete(
                (success, error) -> latch.record(Boolean.TRUE.equals(success))
            );
        }
        return latch.await();
    }

    private CompletableFuture<Boolean> writeOne(String key, int replica, byte[] value, boolean delete) {
        if (replica == localPort) {
            return CompletableFuture.supplyAsync(() -> localWrite(key, value, delete));
        }
        if (delete) {
            return replicaClient.deleteAsync(replica, key);
        }
        return replicaClient.putAsync(replica, key, value);
    }

    private static ReplicaClient.GetResult resultOrFailure(ReplicaClient.@Nullable GetResult result) {
        if (result == null) {
            return ReplicaClient.GetResult.failed();
        }
        return result;
    }

    private ReplicaClient.GetResult localGet(String key) {
        try {
            return ReplicaClient.GetResult.found(dao.get(key));
        } catch (NoSuchElementException exception) {
            return ReplicaClient.GetResult.missing();
        } catch (IOException exception) {
            return ReplicaClient.GetResult.failed();
        }
    }

    private boolean localWrite(String key, byte[] value, boolean delete) {
        try {
            if (delete) {
                dao.delete(key);
            } else {
                dao.upsert(key, value);
            }
            return true;
        } catch (IOException exception) {
            return false;
        }
    }

    private static boolean isReplicaRequest(HttpExchange exchange) {
        return ReplicaClient.REPLICA_VALUE.equals(
            exchange.getRequestHeaders().getFirst(ReplicaClient.REPLICA_HEADER)
        );
    }
}
