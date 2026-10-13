package company.vk.edu.distrib.compute.cloudyy74.kv;

import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;

public class Cloudyy74ClusterRemoteDao implements Dao<String> {
    private static final int MIN_CLUSTER_SIZE = 1;
    private static final int MIN_PORT = 1;
    private static final int MAX_PORT = 65535;

    private final List<Node> nodes;

    public Cloudyy74ClusterRemoteDao(int... ports) {
        if (ports.length < MIN_CLUSTER_SIZE) {
            throw new IllegalArgumentException("Empty cluster configuration");
        }
        for (int port : ports) {
            if (port < MIN_PORT || port > MAX_PORT) {
                throw new IllegalArgumentException("Port out of range");
            }
        }
        this.nodes = Arrays.stream(ports)
                .distinct()
                .sorted()
                .mapToObj(port -> new Node(port, new Cloudyy74RemoteDao(port)))
                .toList();
    }

    @Override
    public String get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        return getNode(key).dao().get(key);
    }

    @Override
    public void upsert(String key, String value) throws IllegalArgumentException, IOException {
        getNode(key).dao().upsert(key, value);
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        getNode(key).dao().delete(key);
    }

    @Override
    public void close() {
        nodes.forEach(node -> node.dao().close());
    }

    private Node getNode(String key) {
        if (key.isEmpty()) {
            throw new IllegalArgumentException("Empty key");
        }
        Node selected = nodes.getFirst();
        long bestScore = hash(key, selected.port());
        for (Node node : nodes) {
            final var score = hash(key, node.port());
            if (Long.compareUnsigned(score, bestScore) > 0) {
                selected = node;
                bestScore = score;
            }
        }
        return selected;
    }

    private static long hash(String key, int port) {
        try {
            final var digest = MessageDigest.getInstance("SHA-256");
            final var value = key + ":" + port;
            final var bytes = digest.digest(value.getBytes(StandardCharsets.UTF_8));
            return ByteBuffer.wrap(bytes).getLong();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is not available", e);
        }
    }

    private record Node(int port, Cloudyy74RemoteDao dao) {
    }
}
