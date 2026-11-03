package company.vk.edu.distrib.compute.artyompeshkov.kv;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import company.vk.edu.distrib.compute.Dao;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ReplicaCoordinator {
    private static final String CLUSTER_PROPERTY = "CLUSTER";
    private static final String REPLICATION_FACTOR_PROPERTY = "REPLICATION_FACTOR";
    private static final Duration TIMEOUT = Duration.ofSeconds(1);
    private static final Logger log = LoggerFactory.getLogger(ReplicaCoordinator.class);

    private final int port;
    private final Dao<byte[]> dao;
    private final HttpClient client = HttpClient.newBuilder().connectTimeout(TIMEOUT).build();

    ReplicaCoordinator(int port, Dao<byte[]> dao) {
        this.port = port;
        this.dao = dao;
    }

    List<Integer> replicasOf(String key) {
        String cluster = System.getProperty(CLUSTER_PROPERTY, "");
        if (cluster.isBlank()) {
            return List.of();
        }
        List<Integer> nodes = Arrays.stream(cluster.split(",", -1)).map(String::trim).map(Integer::valueOf).toList();
        if (nodes.stream().anyMatch(node -> node < 1 || node > 65535)
                || nodes.stream().distinct().count() != nodes.size()) {
            throw new IllegalArgumentException("Cluster must contain distinct ports between 1 and 65535");
        }
        String factor = System.getProperty(REPLICATION_FACTOR_PROPERTY);
        int replicas = factor == null ? nodes.size() : Integer.parseInt(factor);
        if (replicas < 1 || replicas > nodes.size()) {
            throw new IllegalArgumentException("Replication factor must be between 1 and cluster size");
        }
        return Rendezvous.rank(key, nodes).subList(0, replicas);
    }

    byte[] process(String method, String key, byte[] value, int ack, List<Integer> replicas) throws IOException {
        BlockingQueue<CompletableFuture<HttpResponse<byte[]>>> replies = send(method, key, value, replicas);
        int acks = 0;
        int left = replicas.size();
        byte[] found = null;
        if (replicas.contains(port)) {
            left--;
            try {
                found = applyLocally(method, key, value);
                acks++;
            } catch (NoSuchElementException e) {
                acks++;
            } catch (IOException e) {
                log.error("Local replica failed", e);
            }
        }
        while (acks < ack && acks + left >= ack) {
            HttpResponse<byte[]> reply = poll(replies);
            left--;
            if (reply != null && isAck(method, reply.statusCode())) {
                acks++;
                if (reply.statusCode() == HttpURLConnection.HTTP_OK) {
                    found = reply.body();
                }
            }
        }
        if (acks < ack) {
            throw new IOException("Not enough replica acknowledgements");
        }
        return result(method, found);
    }

    void close() {
        client.shutdownNow();
    }

    private BlockingQueue<CompletableFuture<HttpResponse<byte[]>>> send(
            String method, String key, byte[] value, List<Integer> replicas) {
        BlockingQueue<CompletableFuture<HttpResponse<byte[]>>> replies = new LinkedBlockingQueue<>();
        for (int replica : replicas) {
            if (replica != port) {
                CompletableFuture<HttpResponse<byte[]>> reply = client.sendAsync(
                        request(replica, method, key, value), HttpResponse.BodyHandlers.ofByteArray());
                reply.whenComplete((response, error) -> replies.add(reply));
            }
        }
        return replies;
    }

    private static byte[] result(String method, byte @Nullable [] found) {
        if ("GET".equals(method)) {
            if (found == null) {
                throw new NoSuchElementException("Key not found");
            }
            return found;
        }
        return new byte[0];
    }

    byte[] applyLocally(String method, String key, byte[] value) throws IOException {
        switch (method) {
            case "GET" -> {
                return dao.get(key);
            }
            case "PUT" -> dao.upsert(key, value);
            case "DELETE" -> dao.delete(key);
            default -> throw new IllegalArgumentException("Unsupported method: " + method);
        }
        return new byte[0];
    }

    private static boolean isAck(String method, int status) {
        return switch (method) {
            case "GET" -> status == HttpURLConnection.HTTP_OK || status == HttpURLConnection.HTTP_NOT_FOUND;
            case "PUT" -> status == HttpURLConnection.HTTP_CREATED;
            case "DELETE" -> status == HttpURLConnection.HTTP_ACCEPTED;
            default -> false;
        };
    }

    private static @Nullable HttpResponse<byte[]> poll(
            BlockingQueue<CompletableFuture<HttpResponse<byte[]>>> replies) throws IOException {
        try {
            CompletableFuture<HttpResponse<byte[]>> reply = replies.poll(
                    TIMEOUT.multipliedBy(2).toMillis(), TimeUnit.MILLISECONDS);
            if (reply == null) {
                throw new IOException("Timed out waiting for replicas");
            }
            return reply.join();
        } catch (CompletionException e) {
            log.debug("Replica did not answer", e);
            return null;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while waiting for replicas", e);
        }
    }

    private static HttpRequest request(int replica, String method, String key, byte[] value) {
        URI uri = URI.create("http://localhost:" + replica + "/internal/entity?id="
                + URLEncoder.encode(key, StandardCharsets.UTF_8));
        HttpRequest.BodyPublisher body = "PUT".equals(method)
                ? HttpRequest.BodyPublishers.ofByteArray(value)
                : HttpRequest.BodyPublishers.noBody();
        return HttpRequest.newBuilder(uri).method(method, body).timeout(TIMEOUT).build();
    }
}
