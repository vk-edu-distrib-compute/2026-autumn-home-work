package company.vk.edu.distrib.compute.ddkudrin.kv;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;

public final class DDKudrinNodeRouter {
    private static final int DEFAULT_VIRTUAL_NODE_COUNT = 8;
    private static final String HASH_ALGORITHM = "SHA-256";

    public enum Algorithm {
        CONSISTENT,
        RENDEZVOUS
    }

    private final int[] ports;
    private final Algorithm algorithm;
    private final NavigableMap<Long, Integer> ring = new TreeMap<>();

    public DDKudrinNodeRouter(Algorithm algorithm, int... ports) {
        if (ports.length == 0) {
            throw new IllegalArgumentException("Expected at least one KV service port");
        }
        this.algorithm = Objects.requireNonNull(algorithm);
        this.ports = ports.clone();
        if (algorithm == Algorithm.CONSISTENT) {
            int virtualNodeCount = getVirtualNodeCount();
            for (int i = 0; i < ports.length; i++) {
                for (int replica = 0; replica < virtualNodeCount; replica++) {
                    long position = hash(ports[i] + ":" + replica);
                    ring.merge(position, i, (left, right) -> this.ports[left] <= this.ports[right] ? left : right);
                }
            }
        }
    }

    private static int getVirtualNodeCount() {
        String value = System.getenv("VIRTUAL_NODE_COUNT");
        if (value == null) {
            return DEFAULT_VIRTUAL_NODE_COUNT;
        }
        try {
            int count = Integer.parseInt(value);
            if (count <= 0) {
                throw new IllegalArgumentException("VIRTUAL_NODE_COUNT must be a positive integer");
            }
            return count;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("VIRTUAL_NODE_COUNT must be a positive integer", e);
        }
    }

    public int selectNode(String key) {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("Key must not be empty");
        }
        return switch (algorithm) {
            case CONSISTENT -> selectConsistent(key);
            case RENDEZVOUS -> selectRendezvous(key);
        };
    }

    private int selectConsistent(String key) {
        var entry = ring.ceilingEntry(hash(key));
        if (entry == null) {
            entry = ring.firstEntry();
        }
        return Objects.requireNonNull(entry).getValue();
    }

    private int selectRendezvous(String key) {
        int selected = 0;
        long bestScore = hash(ports[0] + ":" + key);
        for (int i = 1; i < ports.length; i++) {
            long score = hash(ports[i] + ":" + key);
            if (score > bestScore || (score == bestScore && ports[i] < ports[selected])) {
                selected = i;
                bestScore = score;
            }
        }
        return selected;
    }

    private static long hash(String value) {
        try {
            MessageDigest digest = MessageDigest.getInstance(HASH_ALGORITHM);
            byte[] hash = digest.digest(value.getBytes(StandardCharsets.UTF_8));
            return ByteBuffer.wrap(hash).getLong();
        } catch (NoSuchAlgorithmException e) {
            throw new AssertionError(e);
        }
    }
}
