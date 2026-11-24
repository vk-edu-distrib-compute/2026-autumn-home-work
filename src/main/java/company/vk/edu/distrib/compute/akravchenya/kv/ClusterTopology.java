package company.vk.edu.distrib.compute.akravchenya.kv;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Снапшот топологии кластера: список узлов и фактор репликации.
 */
final class ClusterTopology {

    private static final String CLUSTER_PROPERTY = "CLUSTER";
    private static final String REPLICATION_FACTOR_PROPERTY = "kv.replicationFactor";
    private static final long FNV_OFFSET_BASIS = 0xcbf29ce484222325L;
    private static final long FNV_PRIME = 0x100000001b3L;

    private final int selfPort;
    private final List<Integer> nodes;
    private final int replicationFactor;

    private ClusterTopology(int selfPort, List<Integer> nodes, int replicationFactor) {
        this.selfPort = selfPort;
        this.nodes = nodes;
        this.replicationFactor = replicationFactor;
    }

    static ClusterTopology read(int selfPort) {
        var nodes = parseClusterNodes(selfPort);
        return new ClusterTopology(selfPort, nodes, replicationFactor(nodes.size()));
    }

    int size() {
        return nodes.size();
    }

    int replicationFactor() {
        return replicationFactor;
    }

    List<Integer> replicasOf(String key) {
        if (replicationFactor >= nodes.size()) {
            return nodes;
        }
        return nodes.stream()
            .sorted(Comparator.comparingLong((Integer port) -> score(key, port)).reversed())
            .limit(replicationFactor)
            .toList();
    }

    private static List<Integer> parseClusterNodes(int selfPort) {
        var property = System.getProperty(CLUSTER_PROPERTY);
        if (property == null || property.isBlank()) {
            return List.of(selfPort);
        }
        var nodes = new ArrayList<Integer>();
        for (var token : property.split(",")) {
            var port = parsePort(token.trim());
            if (port != null && !nodes.contains(port)) {
                nodes.add(port);
            }
        }
        if (nodes.isEmpty()) {
            return List.of(selfPort);
        }
        return nodes;
    }

    private static @Nullable Integer parsePort(String token) {
        try {
            return Integer.valueOf(token);
        } catch (NumberFormatException exception) {
            return null;
        }
    }

    private static int replicationFactor(int clusterSize) {
        var property = System.getProperty(REPLICATION_FACTOR_PROPERTY);
        if (property == null || property.isBlank()) {
            return clusterSize;
        }
        try {
            var factor = Integer.parseInt(property.trim());
            if (factor >= 1 && factor <= clusterSize) {
                return factor;
            }
        } catch (NumberFormatException ignored) {
            // используем размер кластера
        }
        return clusterSize;
    }

    private static long score(String key, int port) {
        return hash64(key + '\u0000' + port);
    }

    private static long hash64(String value) {
        var hash = FNV_OFFSET_BASIS;
        for (var i = 0; i < value.length(); i++) {
            hash ^= value.charAt(i);
            hash *= FNV_PRIME;
        }
        return hash;
    }

    private boolean isSelf(int port) {
        return port == selfPort;
    }

    boolean containsSelf() {
        return nodes.stream().anyMatch(this::isSelf);
    }
}
