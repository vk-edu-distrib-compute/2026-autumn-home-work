package company.vk.edu.distrib.compute.cloudyy74.kv;

import java.util.Arrays;
import java.util.List;

record Cloudyy74ClusterConfig(List<Integer> ports, int replicationFactor, boolean replicated) {
    private static final int MIN_PORT = 1;
    private static final int MAX_PORT = 65535;
    private static final int MIN_REPLICAS = 1;

    static Cloudyy74ClusterConfig fromProperties(int port) {
        final var cluster = System.getProperty("CLUSTER", "");
        if (cluster.isBlank()) {
            return new Cloudyy74ClusterConfig(List.of(port), MIN_REPLICAS, false);
        }
        final var ports = Arrays.stream(cluster.split(",", -1))
                .map(String::trim)
                .map(Integer::parseInt)
                .distinct()
                .sorted()
                .toList();
        if (!ports.contains(port) || ports.stream().anyMatch(value -> value < MIN_PORT || value > MAX_PORT)) {
            throw new IllegalArgumentException("Invalid cluster configuration");
        }
        final var factor = Integer.parseInt(System.getProperty("REPLICATION_FACTOR", Integer.toString(ports.size())));
        if (factor < MIN_REPLICAS || factor > ports.size()) {
            throw new IllegalArgumentException("Invalid replication factor");
        }
        return new Cloudyy74ClusterConfig(ports, factor, true);
    }

    List<Integer> replicas(String key) {
        return ports.stream()
                .sorted((first, second) -> {
                    final var comparison = Long.compareUnsigned(
                            Cloudyy74HashUtils.hash(key, second), Cloudyy74HashUtils.hash(key, first));
                    return comparison == 0 ? Integer.compare(first, second) : comparison;
                })
                .limit(replicationFactor)
                .toList();
    }
}
