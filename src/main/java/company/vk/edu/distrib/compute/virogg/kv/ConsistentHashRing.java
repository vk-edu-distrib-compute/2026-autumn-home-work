package company.vk.edu.distrib.compute.virogg.kv;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.NavigableMap;
import java.util.TreeMap;

final class ConsistentHashRing {
    private static final int VIRTUAL_NODES = 128;
    private static final int MIN_PORT = 1;
    private static final int MAX_PORT = 65535;
    private final NavigableMap<Long, Integer> ring = new TreeMap<>(Long::compareUnsigned);

    ConsistentHashRing(int... ports) {
        int[] sortedPorts = ports.clone();
        Arrays.sort(sortedPorts);
        if (sortedPorts.length == 0) {
            throw new IllegalArgumentException("Cluster must have at least one port");
        }
        validateSortedPorts(sortedPorts);
        for (int port : sortedPorts) {
            for (int replica = 0; replica < VIRTUAL_NODES; replica++) {
                ring.put(hash(port + ":" + replica), port);
            }
        }
    }

    int portFor(String key) {
        if (key.isEmpty()) {
            throw new IllegalArgumentException("Key must not be empty");
        }
        var owner = ring.ceilingEntry(hash(key));
        if (owner == null) {
            owner = ring.firstEntry();
        }
        return owner.getValue();
    }

    private static void validateSortedPorts(int... sortedPorts) {
        for (int i = 0; i < sortedPorts.length; i++) {
            int port = sortedPorts[i];
            if (port < MIN_PORT || port > MAX_PORT) {
                throw new IllegalArgumentException("Port must be between 1 and 65535");
            }
            if (i > 0 && port == sortedPorts[i - 1]) {
                throw new IllegalArgumentException("Cluster ports must be unique");
            }
        }
    }

    private static long hash(String value) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return ByteBuffer.wrap(digest.digest(value.getBytes(StandardCharsets.UTF_8))).getLong();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
    }
}
