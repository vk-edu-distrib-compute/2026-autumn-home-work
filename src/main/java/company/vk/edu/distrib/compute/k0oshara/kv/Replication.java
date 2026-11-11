package company.vk.edu.distrib.compute.k0oshara.kv;

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
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

final class Replication implements AutoCloseable {
    private static final String CLUSTER = "CLUSTER";
    private static final String REPLICATION_FACTOR = "REPLICATION_FACTOR";
    private static final String REPLICA_HEADER = "X-K0oshara-Replica";
    private static final int OK = 200;
    private static final int BAD_REQUEST = 400;
    private static final int NOT_FOUND = 404;
    private static final int METHOD_NOT_ALLOWED = 405;
    private static final int CREATED = 201;
    private static final int ACCEPTED = 202;
    private static final int INTERNAL_ERROR = 503;
    private static final int MIN_ACK = 1;
    private static final int MIN_PORT = 1;
    private static final int MAX_PORT = 65535;
    private static final Duration CONNECT_TIMEOUT = Duration.ofMillis(300);
    private static final Duration REQUEST_TIMEOUT = Duration.ofMillis(700);

    private final int localPort;
    private final Dao<byte[]> dao;
    private final HttpClient client;

    Replication(int localPort, Dao<byte[]> dao) {
        this.localPort = localPort;
        this.dao = dao;
        client = HttpClient.newBuilder().connectTimeout(CONNECT_TIMEOUT).build();
    }

    CompletableFuture<Reply> execute(String method, String key, String ack, byte[] body, boolean internal) {
        int requestedAck = parseAck(ack);
        if (requestedAck < MIN_ACK) {
            return CompletableFuture.completedFuture(new Reply(BAD_REQUEST, null));
        }
        Config config;
        try {
            config = Config.read(localPort);
        } catch (IllegalArgumentException exception) {
            return CompletableFuture.completedFuture(new Reply(INTERNAL_ERROR, null));
        }
        if (requestedAck > config.replicationFactor) {
            return CompletableFuture.completedFuture(new Reply(BAD_REQUEST, null));
        }
        if (internal || !config.clustered) {
            return CompletableFuture.completedFuture(localReply(method, key, body));
        }
        return executeReplicas(method, key, requestedAck, body, config);
    }

    @Override
    public void close() {
        client.shutdownNow();
    }

    private CompletableFuture<Reply> executeReplicas(String method, String key, int ack, byte[] body, Config config) {
        int[] replicas = selectReplicas(key, config);
        List<CompletableFuture<Reply>> operations = new ArrayList<>(replicas.length);
        CompletableFuture<Reply> localOperation = new CompletableFuture<>();
        for (int port : replicas) {
            if (port == localPort) {
                operations.add(localOperation);
            } else {
                operations.add(sendReplica(port, method, key, body));
            }
        }
        if (operations.contains(localOperation)) {
            localOperation.complete(localReply(method, key, body));
        }
        Quorum quorum = new Quorum(method, ack, operations.size());
        for (CompletableFuture<Reply> operation : operations) {
            operation.whenComplete(quorum::accept);
        }
        return quorum.result;
    }

    private int[] selectReplicas(String key, Config config) {
        int primary = RemoteLinksDao.choosePort(key, config.ports);
        int primaryIndex = Arrays.binarySearch(config.ports, primary);
        int[] replicas = new int[config.replicationFactor];
        for (int i = 0; i < replicas.length; i++) {
            replicas[i] = config.ports[(primaryIndex + i) % config.ports.length];
        }
        return replicas;
    }

    private CompletableFuture<Reply> sendReplica(int port, String method, String key, byte[] body) {
        String encodedKey = URLEncoder.encode(key, StandardCharsets.UTF_8);
        URI uri = URI.create("http://localhost:" + port + "/v0/entity?id=" + encodedKey + "&ack=1");
        HttpRequest.Builder request = HttpRequest.newBuilder(uri)
            .timeout(REQUEST_TIMEOUT)
            .header(REPLICA_HEADER, "true");
        try {
            return client.sendAsync(
                request.method(method, body == null
                    ? HttpRequest.BodyPublishers.noBody()
                    : HttpRequest.BodyPublishers.ofByteArray(body)).build(),
                HttpResponse.BodyHandlers.ofByteArray())
                .orTimeout(REQUEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
                .thenApply(response -> new Reply(response.statusCode(), response.body()));
        } catch (RuntimeException exception) {
            return CompletableFuture.failedFuture(exception);
        }
    }

    private Reply localReply(String method, String key, byte[] body) {
        try {
            return switch (method) {
                case "GET" -> new Reply(OK, dao.get(key));
                case "PUT" -> {
                    dao.upsert(key, body == null ? new byte[0] : body);
                    yield new Reply(CREATED, null);
                }
                case "DELETE" -> {
                    dao.delete(key);
                    yield new Reply(ACCEPTED, null);
                }
                default -> new Reply(METHOD_NOT_ALLOWED, null);
            };
        } catch (NoSuchElementException exception) {
            return new Reply(NOT_FOUND, null);
        } catch (IOException | RuntimeException exception) {
            return new Reply(INTERNAL_ERROR, null);
        }
    }

    private int parseAck(String ack) {
        if (ack == null) {
            return MIN_ACK;
        }
        try {
            return Integer.parseInt(ack);
        } catch (NumberFormatException exception) {
            return 0;
        }
    }

    private boolean isSuccessful(String method, Reply reply) {
        return switch (method) {
            case "GET" -> reply.status == OK || reply.status == NOT_FOUND;
            case "PUT" -> reply.status == CREATED;
            case "DELETE" -> reply.status == ACCEPTED;
            default -> false;
        };
    }

    private Reply quorumReply(String method, byte[] value) {
        return switch (method) {
            case "GET" -> value != null ? new Reply(OK, value) : new Reply(NOT_FOUND, null);
            case "PUT" -> new Reply(CREATED, null);
            case "DELETE" -> new Reply(OK, null);
            default -> new Reply(INTERNAL_ERROR, null);
        };
    }

    private record Config(int[] ports, int replicationFactor, boolean clustered) {
        private static Config read(int localPort) {
            String cluster = propertyOrEnvironment(CLUSTER);
            if (cluster == null || cluster.isBlank()) {
                return new Config(new int[]{localPort}, MIN_ACK, false);
            }
            int[] ports = parsePorts(cluster);
            return new Config(ports, readFactor(ports.length), true);
        }

        private static int readFactor(int portCount) {
            String value = propertyOrEnvironment(REPLICATION_FACTOR);
            int factor = value == null || value.isBlank() ? portCount : Integer.parseInt(value.trim());
            if (factor < MIN_ACK || factor > portCount) {
                throw new IllegalArgumentException("Invalid replication factor");
            }
            return factor;
        }

        private static int[] parsePorts(String cluster) {
            int[] ports = Arrays.stream(cluster.split(",", -1))
                .map(String::trim)
                .mapToInt(Integer::parseInt)
                .distinct()
                .sorted()
                .toArray();
            for (int port : ports) {
                if (port < MIN_PORT || port > MAX_PORT) {
                    throw new IllegalArgumentException("Invalid cluster port");
                }
            }
            return ports;
        }

        private static String propertyOrEnvironment(String name) {
            String value = System.getProperty(name);
            return value == null ? System.getenv(name) : value;
        }
    }

    record Reply(int status, byte[] body) {
    }

    private final class Quorum {
        private final String method;
        private final int ack;
        private final CompletableFuture<Reply> result = new CompletableFuture<>();
        private final ReentrantLock lock = new ReentrantLock();
        private boolean finished;
        private int remaining;
        private int successfulResponses;
        private byte[] value;

        private Quorum(String method, int ack, int total) {
            this.method = method;
            this.ack = ack;
            remaining = total;
        }

        private void accept(Reply reply, Throwable failure) {
            Reply finalReply = null;
            lock.lock();
            try {
                if (finished) {
                    return;
                }
                remaining--;
                if (failure == null && isSuccessful(method, reply)) {
                    successfulResponses++;
                    if (reply.status == OK) {
                        value = reply.body;
                    }
                }
                if (successfulResponses >= ack) {
                    finished = true;
                    finalReply = quorumReply(method, value);
                } else if (successfulResponses + remaining < ack) {
                    finished = true;
                    finalReply = new Reply(INTERNAL_ERROR, null);
                }
            } finally {
                lock.unlock();
            }
            if (finalReply != null) {
                result.complete(finalReply);
            }
        }
    }
}
