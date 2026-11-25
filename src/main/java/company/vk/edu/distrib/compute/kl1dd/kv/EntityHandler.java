package company.vk.edu.distrib.compute.kl1dd.kv;

import com.sun.net.httpserver.HttpExchange;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class EntityHandler {
    private static final int HTTP_OK = 200;
    private static final int HTTP_ACCEPTED = 202;
    private static final int HTTP_NOT_FOUND = 404;
    private static final int HTTP_UNAVAILABLE = 503;

    private final DaoHelper daoHelper;
    private final ReplicationImpl replication;
    private final ReplicationClient replicationClient;
    private final int currentPort;
    private final ReplicaSelector replicaSelector = new ReplicaSelector();

    EntityHandler(DaoHelper daoHelper, ReplicationImpl replication, ReplicationClient replicationClient,
                  int currentPort) {
        this.daoHelper = daoHelper;
        this.replication = replication;
        this.replicationClient = replicationClient;
        this.currentPort = currentPort;
    }

    // DELETE
    void handleDelete(HttpExchange exchange, String id, int ack, boolean ackSpecified,
                      int... clusterPorts) throws IOException {
        int[] replicaPorts = replicaSelector.selectReplicas(id, clusterPorts.length, clusterPorts);

        int successCount = 0;
        List<CompletableFuture<Boolean>> futures = new ArrayList<>();
        for (int port : replicaPorts) {
            if (port == currentPort) {
                if (daoHelper.delete(id)) {
                    successCount++;
                }
            } else {
                futures.add(replicationClient.deleteFromReplica(port, id));
            }
        }

        boolean flag = replication.waitForAck(futures, successCount, ack);
        exchange.sendResponseHeaders(responseStatusDelete(flag, ackSpecified), -1);
        exchange.close();
    }

    void handleLocalDelete(HttpExchange exchange, String id) throws IOException {
        if (daoHelper.delete(id)) {
            exchange.sendResponseHeaders(202, -1);
            exchange.close();
        } else {
            exchange.sendResponseHeaders(503, -1);
            exchange.close();
        }
    }

    private int responseStatusDelete(boolean flag, boolean ackSpecified) {
        if (flag) {
            if (ackSpecified) {
                return HTTP_OK;
            }
            return HTTP_ACCEPTED;
        }

        return HTTP_UNAVAILABLE;
    }

    // PUT
    void handlePut(HttpExchange exchange, String id, byte[] value, int ack, int... clusterPorts) throws IOException {
        int[] replicaPorts = replicaSelector.selectReplicas(id, clusterPorts.length, clusterPorts);

        int successCount = 0;
        List<CompletableFuture<Boolean>> futures = new ArrayList<>();
        for (int port : replicaPorts) {
            if (port == currentPort) {
                if (daoHelper.upsert(id, value)) {
                    successCount++;
                }
            } else {
                futures.add(replicationClient.putToReplica(port, id, value));
            }
        }

        boolean flag = replication.waitForAck(futures, successCount, ack);
        if (flag) {
            exchange.sendResponseHeaders(201, -1);
        } else {
            exchange.sendResponseHeaders(503, -1);
        }
        exchange.close();
    }

    void handleLocalPut(HttpExchange exchange, String id, byte[] value) throws IOException {
        if (daoHelper.upsert(id, value)) {
            exchange.sendResponseHeaders(201, -1);
            exchange.close();
        } else {
            exchange.sendResponseHeaders(503, -1);
            exchange.close();
        }
    }

    // GET
    void handleGet(HttpExchange exchange, String id, int ack, int... clusterPorts) throws IOException {
        int[] replicaPorts = replicaSelector.selectReplicas(id, clusterPorts.length, clusterPorts);

        ReplicationImpl.GetReplicaResult localResult;
        boolean currentIsReplica = false;
        for (int port : replicaPorts) {
            if (port == currentPort) {
                currentIsReplica = true;
                break;
            }
        }
        if (currentIsReplica) {
            localResult = daoHelper.getResult(id);
        } else {
            localResult = new ReplicationImpl.GetReplicaResult(HTTP_UNAVAILABLE, null);
        }

        List<CompletableFuture<ReplicationImpl.GetReplicaResult>> futures = new ArrayList<>();
        for (int port : replicaPorts) {
            if (port != currentPort) {
                futures.add(replicationClient.getFromReplica(port, id));
            }
        }

        ReplicationImpl.GetReplicaResult result = replication.waitForGetAck(futures, localResult, ack);
        int resultStatusCode = result.getStatusCode();
        if (resultStatusCode == HTTP_OK) {
            byte[] resultValue = result.getValue();
            exchange.sendResponseHeaders(HTTP_OK, resultValue.length);
            exchange.getResponseBody().write(resultValue);
            exchange.close();
        } else if (resultStatusCode == HTTP_NOT_FOUND) {
            exchange.sendResponseHeaders(404, -1);
            exchange.close();
        } else {
            exchange.sendResponseHeaders(503, -1);
            exchange.close();
        }
    }

    void handleLocalGet(HttpExchange exchange, String id) throws IOException {
        ReplicationImpl.GetReplicaResult result = daoHelper.getResult(id);
        int resultStatusCode = result.getStatusCode();
        if (resultStatusCode == HTTP_OK) {
            byte[] resultValue = result.getValue();

            exchange.sendResponseHeaders(200, resultValue.length);
            exchange.getResponseBody().write(resultValue);
            exchange.close();
        } else if (resultStatusCode == HTTP_NOT_FOUND) {
            exchange.sendResponseHeaders(404, -1);
            exchange.close();
        } else if (resultStatusCode == HTTP_UNAVAILABLE) {
            exchange.sendResponseHeaders(503, -1);
            exchange.close();
        }
    }
}
