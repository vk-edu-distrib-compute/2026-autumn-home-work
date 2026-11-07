package company.vk.edu.distrib.compute.virogg.kv;

import org.jspecify.annotations.Nullable;

final class ReplicationConfig {
    private final ConsistentHashRing ring;
    private final int replicationFactor;
    private final boolean standalone;

    private ReplicationConfig(ConsistentHashRing ring, int replicationFactor, boolean standalone) {
        this.ring = ring;
        this.replicationFactor = replicationFactor;
        this.standalone = standalone;
    }

    static ReplicationConfig parse(int localPort, @Nullable String cluster, @Nullable String factor) {
        int[] ports;
        if (cluster == null) {
            ports = new int[]{localPort};
        } else {
            String[] configuredPorts = cluster.split(",", -1);
            ports = new int[configuredPorts.length];
            for (int i = 0; i < configuredPorts.length; i++) {
                ports[i] = Integer.parseInt(configuredPorts[i].trim());
            }
        }
        ConsistentHashRing ring = new ConsistentHashRing(ports);
        int replicationFactor = factor == null ? ports.length : Integer.parseInt(factor);
        if (replicationFactor < 1 || replicationFactor > ports.length) {
            throw new IllegalArgumentException("Replication factor must fit the cluster");
        }
        return new ReplicationConfig(ring, replicationFactor, cluster == null);
    }

    boolean isStandalone() {
        return standalone;
    }

    int replicationFactor() {
        return replicationFactor;
    }

    int[] portsFor(String key) {
        return ring.portsFor(key, replicationFactor);
    }
}
