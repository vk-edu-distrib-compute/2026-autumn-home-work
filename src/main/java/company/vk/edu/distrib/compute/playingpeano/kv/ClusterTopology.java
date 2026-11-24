package company.vk.edu.distrib.compute.playingpeano.kv;

import java.util.Arrays;

import org.jspecify.annotations.Nullable;

final class ClusterTopology {
    private static final String CLUSTER_PROPERTY = "CLUSTER";
    private static final String PORT_SEPARATOR = ",";
    private static final long FNV_OFFSET = 0xcbf29ce484222325L;
    private static final long FNV_PRIME = 0x100000001b3L;
    private static final int NOT_HIGHER = 0;

    private final int[] ports;

    private ClusterTopology(int selfPort) {
        ports = parsePorts(selfPort);
    }

    static boolean isConfigured() {
        @Nullable String cluster = System.getProperty(CLUSTER_PROPERTY);
        return cluster != null && !cluster.isBlank();
    }

    static ClusterTopology current(int selfPort) {
        return new ClusterTopology(selfPort);
    }

    int size() {
        return ports.length;
    }

    int[] replicasFor(String key) {
        int[] ordered = ports.clone();
        Arrays.sort(ordered);
        int start = indexOf(ordered, primaryPort(key));
        int[] replicas = new int[ordered.length];
        for (int index = 0; index < ordered.length; index++) {
            replicas[index] = ordered[(start + index) % ordered.length];
        }
        return replicas;
    }

    private int primaryPort(String key) {
        int owner = ports[0];
        long ownerScore = rendezvousScore(key, owner);
        for (int index = 1; index < ports.length; index++) {
            int candidate = ports[index];
            long candidateScore = rendezvousScore(key, candidate);
            if (outranks(candidateScore, ownerScore)) {
                owner = candidate;
                ownerScore = candidateScore;
            }
        }
        return owner;
    }

    private static int[] parsePorts(int selfPort) {
        @Nullable String cluster = System.getProperty(CLUSTER_PROPERTY);
        if (cluster == null || cluster.isBlank()) {
            return new int[] {selfPort};
        }
        String[] parts = cluster.split(PORT_SEPARATOR);
        int[] parsed = new int[parts.length];
        for (int index = 0; index < parts.length; index++) {
            parsed[index] = Integer.parseInt(parts[index].trim());
        }
        return parsed;
    }

    private static int indexOf(int[] ports, int port) {
        for (int index = 0; index < ports.length; index++) {
            if (ports[index] == port) {
                return index;
            }
        }
        return 0;
    }

    private static boolean outranks(long candidateScore, long ownerScore) {
        return Long.compareUnsigned(candidateScore, ownerScore) > NOT_HIGHER;
    }

    private static long rendezvousScore(String key, int port) {
        long hash = FNV_OFFSET;
        for (int index = 0; index < key.length(); index++) {
            hash ^= key.charAt(index);
            hash *= FNV_PRIME;
        }
        hash ^= port & 0xffffffffL;
        hash *= FNV_PRIME;
        return hash;
    }
}
