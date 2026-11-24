package company.vk.edu.distrib.compute.mrglaster.kv.replication.config;

import java.util.Arrays;

public final class ClusterConfigReader {

    private static final String CLUSTER_PROPERTY = "CLUSTER";
    private static final String REPLICATION_FACTOR_PROPERTY = "REPLICATION_FACTOR";
    private static final String CLUSTERS_PORTS_DELIMITER = ",";
    private static final int MIN_PORT = 1;
    private static final int MAX_PORT = 65_535;

    private ClusterConfigReader() {
    }

    public static ClusterConfig read() {
        String cluster = System.getProperty(CLUSTER_PROPERTY);
        if (cluster == null || cluster.isBlank()) {
            return null;
        }
        int[] ports = parsePorts(cluster);
        int n = parseReplicationFactor(ports.length);
        return new ClusterConfig(ports, n);
    }

    private static int[] parsePorts(String cluster) {
        int[] ports = Arrays.stream(cluster.split(CLUSTERS_PORTS_DELIMITER))
                .map(String::trim)
                .mapToInt(Integer::parseInt)
                .toArray();
        for (int p : ports) {
            if (p < MIN_PORT || p > MAX_PORT) {
                throw new IllegalArgumentException("Invalid port: " + p);
            }
        }
        return ports;
    }

    private static int parseReplicationFactor(int clusterSize) {
        String raw = System.getProperty(REPLICATION_FACTOR_PROPERTY);
        int n = (raw == null || raw.isBlank()) ? clusterSize : Integer.parseInt(raw.trim());
        if (n < 1 || n > clusterSize) {
            throw new IllegalArgumentException("Invalid REPLICATION_FACTOR: " + n);
        }
        return n;
    }
}
