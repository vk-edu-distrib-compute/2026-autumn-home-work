package company.vk.edu.distrib.compute.akravchenya.kv;

import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;

/**
 * Координатор реплицируемых операций: определяет набор реплик ключа,
 * рассылает запросы репликам и дожидается кворума подтверждений.
 */
final class ReplicaCoordinator {

    static final String INTERNAL_REQUEST_HEADER = "X-Internal-Request";

    private static final String ENTITY_PATH = "/v0/entity?id=";
    private static final String PUT = "PUT";
    private static final String DELETE = "DELETE";
    private static final Duration REPLICA_TIMEOUT = Duration.ofSeconds(1);
    private static final int OK = 200;
    private static final int CREATED = 201;
    private static final int ACCEPTED = 202;
    private static final int NOT_FOUND = 404;

    private final Dao<byte[]> dao;
    private final int selfPort;
    private final HttpClient client;

    ReplicaCoordinator(Dao<byte[]> dao, int selfPort, HttpClient client) {
        this.dao = dao;
        this.selfPort = selfPort;
        this.client = client;
    }

    boolean replicationEnabled() {
        return ClusterTopology.read(selfPort).size() > 1;
    }

    int ackOf(String rawAck) {
        if (rawAck == null || rawAck.isEmpty()) {
            return 1;
        }
        var factor = ClusterTopology.read(selfPort).replicationFactor();
        int ack;
        try {
            ack = Integer.parseInt(rawAck);
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException("ack must be a number: " + rawAck, exception);
        }
        if (ack < 1 || ack > factor) {
            throw new IllegalArgumentException("ack must be between 1 and " + factor + ": " + ack);
        }
        return ack;
    }

    byte[] getLocal(String key) throws IOException {
        return dao.get(key);
    }

    void upsertLocal(String key, byte[] value) throws IOException {
        dao.upsert(key, value);
    }

    void deleteLocal(String key) throws IOException {
        dao.delete(key);
    }

    byte[] get(String key, int ack) {
        var topology = ClusterTopology.read(selfPort);
        var futures = submitGets(topology.replicasOf(key), key);
        var tally = tallyOf(topology, futures, key, ack);
        if (tally.value() == null) {
            throw new NoSuchElementException("no value stored for key: " + key);
        }
        return tally.value();
    }

    void upsert(String key, byte[] value, int ack) {
        write(key, PUT, value, ack);
    }

    void delete(String key, int ack) {
        write(key, DELETE, null, ack);
    }

    private void write(String key, String method, byte[] value, int ack) {
        var topology = ClusterTopology.read(selfPort);
        var futures = submitWrites(topology.replicasOf(key), key, method, value);
        var confirmations = topology.containsSelf() && localWrite(key, method, value) ? 1 : 0;
        for (var future : futures) {
            if (confirmations >= ack) {
                return;
            }
            if (future.join() == successStatusOf(method)) {
                confirmations++;
            }
        }
        if (confirmations < ack) {
            throw new QuorumNotReachedException("collected " + confirmations + " confirmations out of "
                + ack + " required for key: " + key);
        }
    }

    private ReplicaAnswer tallyOf(ClusterTopology topology, List<CompletableFuture<ReplicaAnswer>> futures,
                                  String key, int ack) {
        var answered = 0;
        byte[] found = null;
        if (topology.containsSelf()) {
            var local = localAnswer(key);
            answered = local.answered() ? 1 : 0;
            found = local.value();
        }
        for (var future : futures) {
            if (answered >= ack) {
                break;
            }
            var answer = future.join();
            if (answer.answered()) {
                answered++;
            }
            if (found == null) {
                found = answer.value();
            }
        }
        if (answered < ack) {
            throw new QuorumNotReachedException("collected " + answered + " answers out of " + ack
                + " required for key: " + key);
        }
        return ReplicaAnswer.found(found);
    }

    private List<CompletableFuture<ReplicaAnswer>> submitGets(List<Integer> replicas, String key) {
        var futures = new ArrayList<CompletableFuture<ReplicaAnswer>>();
        for (var replica : replicas) {
            if (replica != selfPort) {
                futures.add(sendGet(replica, key));
            }
        }
        return futures;
    }

    private List<CompletableFuture<Integer>> submitWrites(List<Integer> replicas, String key,
                                                          String method, byte[] value) {
        var futures = new ArrayList<CompletableFuture<Integer>>();
        for (var replica : replicas) {
            if (replica != selfPort) {
                futures.add(sendWrite(replica, key, method, value));
            }
        }
        return futures;
    }

    private boolean localWrite(String key, String method, byte[] value) {
        try {
            if (DELETE.equals(method)) {
                dao.delete(key);
            } else {
                dao.upsert(key, value);
            }
            return true;
        } catch (IOException | RuntimeException exception) {
            return false;
        }
    }

    private ReplicaAnswer localAnswer(String key) {
        try {
            return ReplicaAnswer.found(dao.get(key));
        } catch (NoSuchElementException exception) {
            return ReplicaAnswer.missing();
        } catch (IOException | RuntimeException exception) {
            return ReplicaAnswer.unknown();
        }
    }

    private CompletableFuture<Integer> sendWrite(int port, String key, String method, byte[] value) {
        var publisher = value == null
            ? HttpRequest.BodyPublishers.noBody()
            : HttpRequest.BodyPublishers.ofByteArray(value);
        var request = internalRequest(port, key)
            .method(method, publisher)
            .build();
        return client.sendAsync(request, HttpResponse.BodyHandlers.discarding())
            .handle((response, error) -> error == null ? response.statusCode() : -1);
    }

    private CompletableFuture<ReplicaAnswer> sendGet(int port, String key) {
        var request = internalRequest(port, key)
            .GET()
            .build();
        return client.sendAsync(request, HttpResponse.BodyHandlers.ofByteArray())
            .handle((response, error) -> {
                if (error != null) {
                    return ReplicaAnswer.unknown();
                }
                return switch (response.statusCode()) {
                    case OK -> ReplicaAnswer.found(response.body());
                    case NOT_FOUND -> ReplicaAnswer.missing();
                    default -> ReplicaAnswer.unknown();
                };
            });
    }

    private static HttpRequest.Builder internalRequest(int port, String key) {
        return HttpRequest.newBuilder()
            .uri(uri(port, key))
            .timeout(REPLICA_TIMEOUT)
            .setHeader(INTERNAL_REQUEST_HEADER, "1");
    }

    private static int successStatusOf(String method) {
        return DELETE.equals(method) ? ACCEPTED : CREATED;
    }

    private static URI uri(int port, String key) {
        var encoded = URLEncoder.encode(key, StandardCharsets.UTF_8);
        return URI.create("http://localhost:" + port + ENTITY_PATH + encoded);
    }

    private record ReplicaAnswer(boolean answered, byte[] value) {

        static ReplicaAnswer found(byte[] value) {
            return new ReplicaAnswer(true, value);
        }

        static ReplicaAnswer missing() {
            return new ReplicaAnswer(true, null);
        }

        static ReplicaAnswer unknown() {
            return new ReplicaAnswer(false, null);
        }
    }
}
