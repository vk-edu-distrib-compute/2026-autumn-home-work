package company.vk.edu.distrib.compute.tomovalex.kv;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public class ReplicaSelector {
    private static final int MIN_PORT = 1;
    private static final int MAX_PORT = 65535;

    private final List<Integer> ports;

    public ReplicaSelector(int... ports) {
        validatePorts(ports);
        this.ports = Arrays.stream(ports).sorted().distinct().boxed().toList();
    }

    public List<Integer> getPorts() {
        return ports;
    }

    public List<Integer> selectReplicas(String key, int count) {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException();
        }
        if (count < 1 || count > ports.size()) {
            throw new IllegalArgumentException("Invalid replica count: " + count);
        }
        List<NodeScore> scores = new ArrayList<>();
        for (int port : ports) {
            scores.add(new NodeScore(port, hash(key + ":" + port)));
        }
        scores.sort(Comparator.comparingLong(NodeScore::score).reversed().thenComparingInt(NodeScore::port));
        return scores.stream().limit(count).map(NodeScore::port).toList();
    }

    private static void validatePorts(int... ports) {
        if (ports == null || ports.length == 0) {
            throw new IllegalArgumentException();
        }
        for (int port : ports) {
            if (port < MIN_PORT || port > MAX_PORT) {
                throw new IllegalArgumentException("Invalid port: " + port);
            }
        }
    }

    private static long hash(String value) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] bytes = digest.digest(value.getBytes(StandardCharsets.UTF_8));
            return ByteBuffer.wrap(bytes).getLong();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
    }

    private record NodeScore(int port, long score) {
    }
}
