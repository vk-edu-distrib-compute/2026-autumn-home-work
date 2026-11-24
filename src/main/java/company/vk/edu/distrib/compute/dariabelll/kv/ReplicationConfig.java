package company.vk.edu.distrib.compute.dariabelll.kv;

import java.util.HashSet;
import java.util.Set;

public final class ReplicationConfig {

    private static final int MAX_PORT = 65535;

    private final int[] ports;
    private final int replicationFactor;

    public ReplicationConfig(int[] ports, int replicationFactor) {
        if (ports.length == 0) {
            throw new IllegalArgumentException("Cluster must contain at least one node");
        }

        this.ports = ports.clone();
        Set<Integer> uniquePorts = new HashSet<>();
        for (int nodePort : this.ports) {
            if (nodePort < 1 || nodePort > MAX_PORT) {
                throw new IllegalArgumentException("Invalid node port: " + nodePort);
            }
            if (!uniquePorts.add(nodePort)) {
                throw new IllegalArgumentException("Duplicate node port: " + nodePort);
            }
        }

        if (replicationFactor < 1 || replicationFactor > this.ports.length) {
            throw new IllegalArgumentException("Invalid replication factor: " + replicationFactor);
        }
        this.replicationFactor = replicationFactor;
    }

    public int[] getPorts() {
        return ports.clone();
    }

    public int getReplicationFactor() {
        return replicationFactor;
    }

}
