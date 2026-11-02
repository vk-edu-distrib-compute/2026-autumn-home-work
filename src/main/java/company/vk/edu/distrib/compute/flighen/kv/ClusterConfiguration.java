package company.vk.edu.distrib.compute.flighen.kv;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public record ClusterConfiguration(List<Integer> ports, int replicaFactor, boolean replicated) {
    private static final String CLUSTER_PROPERTY = "CLUSTER";

    public ClusterConfiguration {
        ports = List.copyOf(ports);
    }

    static ClusterConfiguration fromProperty(int mainPort, int configuredReplicaFactor) {
        String rawCluster = System.getProperty(CLUSTER_PROPERTY);
        if (rawCluster == null || rawCluster.isBlank()) {
            return new ClusterConfiguration(List.of(mainPort), 1, false);
        }

        List<Integer> ports = parsePorts(rawCluster);
        int replicaFactor = configuredReplicaFactor == 0 ? ports.size() : configuredReplicaFactor;
        if (replicaFactor < 1 || replicaFactor > ports.size()) {
            throw new IllegalArgumentException("Replica factor is out of range: " + replicaFactor);
        }

        return new ClusterConfiguration(ports, replicaFactor, true);
    }

    List<Integer> determineReplicas(String key) {
        int primaryPort = determinePrimaryPort(key);
        int primaryIndex = ports.indexOf(primaryPort);
        List<Integer> replicas = new ArrayList<>(replicaFactor);

        for (int offset = 0; offset < replicaFactor; offset++) {
            replicas.add(ports.get((primaryIndex + offset) % ports.size()));
        }

        return replicas;
    }

    private static List<Integer> parsePorts(String rawCluster) {
        List<Integer> ports = new ArrayList<>();
        for (String rawPort : rawCluster.split(",")) {
            int port = Integer.parseInt(rawPort.trim());
            if (port <= 0 || port >= 65536) {
                throw new IllegalArgumentException("Cluster port is out of range: " + port);
            }
            if (!ports.contains(port)) {
                ports.add(port);
            }
        }

        if (ports.isEmpty()) {
            throw new IllegalArgumentException("Cluster must contain at least one node");
        }

        ports.sort(Integer::compareTo);
        return ports;
    }

    private int determinePrimaryPort(String key) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            int selectedPort = -1;
            byte[] maxHash = null;

            for (int port : ports) {
                digest.update(key.getBytes(StandardCharsets.UTF_8));
                digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(port).array());
                byte[] hash = digest.digest();

                if (maxHash == null || Arrays.compareUnsigned(hash, maxHash) > 0) {
                    maxHash = hash;
                    selectedPort = port;
                }
            }

            return selectedPort;
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is not available", e);
        }
    }
}
