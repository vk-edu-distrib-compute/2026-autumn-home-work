package company.vk.edu.distrib.compute.nickmish.kv;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.nickmish.HttpStatus;
import company.vk.edu.distrib.compute.kv.ClusterDaoFactoryTest;
import company.vk.edu.distrib.compute.kv.RemoteDaoFactory;
import org.jspecify.annotations.NullMarked;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.NoSuchElementException;

/**
 * A cluster-aware implementation of {@link RemoteDaoFactory} that distributes keys
 * across multiple nodes using Rendezvous Hashing (Highest Random Weight).
 *
 * <p>Rendezvous hashing ensures that:
 * <ul>
 *   <li>Keys are distributed deterministically across nodes</li>
 *   <li>Adding or removing a node only affects keys that map to that node</li>
 *   <li>The distribution is uniform when using enough keys</li>
 * </ul>
 */
@ClusterDaoFactoryTest
@NullMarked
public final class ClusterRemoteDaoFactory implements RemoteDaoFactory<String> {
    private static final Duration TIMEOUT = Duration.ofSeconds(5);

    @Override
    public Dao<String> create(int... ports) throws IOException {
        if (ports.length == 0) {
            throw new IllegalArgumentException("At least one port must be provided");
        }

        HttpClient client = HttpClient.newBuilder()
                .connectTimeout(TIMEOUT)
                .build();

        return new ClusterRemoteDao(client, ports);
    }

    /**
     * A cluster-aware remote dao that routes requests to the appropriate node
     * based on Rendezvous Hashing.
     */
    private record ClusterRemoteDao(HttpClient client, int[] ports) implements Dao<String> {
        private static final String BASE_URL = "http://localhost";

        @Override
        public String get(String key) throws IOException {
            int port = route(key);
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(BASE_URL + ":" + port + "/v0/entity?id=" + key))
                    .GET()
                    .build();
            try {
                HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
                if (response.statusCode() == HttpStatus.NOT_FOUND.code()) {
                    throw new NoSuchElementException("No value for key: " + key);
                }
                if (response.statusCode() != HttpStatus.OK.code()) {
                    throw new IOException("Unexpected status code: " + response.statusCode());
                }
                return response.body();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Request interrupted", e);
            }
        }

        @Override
        public void upsert(String key, String value) throws IOException {
            int port = route(key);
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(BASE_URL + ":" + port + "/v0/entity?id=" + key))
                    .PUT(HttpRequest.BodyPublishers.ofString(value, StandardCharsets.UTF_8))
                    .build();
            try {
                HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
                if (response.statusCode() != HttpStatus.CREATED.code()) {
                    throw new IOException("Unexpected status code: " + response.statusCode());
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Request interrupted", e);
            }
        }

        @Override
        public void delete(String key) throws IOException {
            int port = route(key);
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(BASE_URL + ":" + port + "/v0/entity?id=" + key))
                    .DELETE()
                    .build();
            try {
                HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
                if (response.statusCode() != HttpStatus.ACCEPTED.code()) {
                    throw new IOException("Unexpected status code: " + response.statusCode());
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Request interrupted", e);
            }
        }

        @Override
        public void close() {
            // HttpClient doesn't need explicit closing
        }

        /**
         * Routes a key to the appropriate node using Rendezvous Hashing (HRW).
         *
         * <p>For each key, we compute a hash for every node and select the node
         * with the highest hash value. This ensures deterministic routing and
         * minimal key movement when nodes are added or removed.
         *
         * @param key the key to route
         * @return the port of the node responsible for the key
         */
        private int route(String key) {
            int selectedPort = ports[0];
            long maxWeight = Long.MIN_VALUE;

            for (int port : ports) {
                long weight = computeWeight(key, port);
                if (weight > maxWeight) {
                    maxWeight = weight;
                    selectedPort = port;
                }
            }

            return selectedPort;
        }

        /**
         * Computes the weight for a key-node pair using SHA-256.
         *
         * <p>The weight is computed as the first 8 bytes of SHA-256(key + ":" + port),
         * interpreted as a long value. This provides a uniform distribution of weights.
         *
         * @param key  the key
         * @param port the node port
         * @return the weight for this key-node pair
         */
        private static long computeWeight(String key, int port) {
            MessageDigest digest;
            try {
                digest = MessageDigest.getInstance("SHA-256");
            } catch (NoSuchAlgorithmException e) {
                throw new IllegalStateException("SHA-256 not available", e);
            }
            byte[] hash = digest.digest((key + ":" + port).getBytes(StandardCharsets.UTF_8));
            // Use first 8 bytes as a long value (big-endian)
            long weight = 0;
            for (int i = 0; i < 8; i++) {
                weight = (weight << 8) | (hash[i] & 0xFF);
            }
            return weight;
        }
    }
}
