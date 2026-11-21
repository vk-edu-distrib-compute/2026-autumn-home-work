package company.vk.edu.distrib.compute.fedorkhokhryakov.kv;

import java.util.Arrays;
import java.util.List;

final class ClusterTopology {
    private final String description;
    private final int[] ports;
    private final RendezvousHashing hashing;

    private ClusterTopology(String description, int... ports) {
        this.description = description;
        this.ports = ports.clone();
        this.hashing = new RendezvousHashing(Arrays.stream(ports).mapToObj(port -> "localhost:" + port).toList());
    }

    static ClusterTopology parse(String description, int selfPort) {
        if (description.isBlank()) {
            return new ClusterTopology(description, selfPort);
        }
        List<Integer> ports = Arrays.stream(description.split(","))
            .map(String::trim)
            .filter(port -> !port.isEmpty())
            .map(Integer::parseInt)
            .distinct()
            .toList();
        return new ClusterTopology(description, ports.stream().mapToInt(Integer::intValue).toArray());
    }

    boolean describedBy(String clusterDescription) {
        return description.equals(clusterDescription);
    }

    int size() {
        return ports.length;
    }

    int[] replicas(String key, int count) {
        return Arrays.stream(hashing.topNodes(key, count)).map(node -> ports[node]).toArray();
    }
}
