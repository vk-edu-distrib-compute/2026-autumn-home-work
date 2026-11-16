package company.vk.edu.distrib.compute.vagifbaratov.kv;

import java.util.Arrays;

public final class ClusterConfigFactory {
    public static final String ENV_CLUSTER = "CLUSTER";
    public static final String ENV_REPLICATION_FACTOR = "REPLICATION_FACTOR";
    public static final String ENV_CLUSTER_HOST = "CLUSTER_HOST";
    public static final String DEFAULT_HOST = "http://localhost";

    private ClusterConfigFactory() {
    }

    static ClusterConfig load(int port) {
        String strPorts = System.getProperty(ENV_CLUSTER, Integer.toString(port));

        int[] ports = Arrays.stream(strPorts.split(","))
                .map(String::trim).mapToInt(Integer::parseInt).distinct().sorted().toArray();

        if (ports.length == 0 || Arrays.stream(ports).anyMatch(p -> p <= 0 || p >= 65536)) {
            throw new IllegalArgumentException("Invalid cluster ports");
        }

        String strReplicas = System.getProperty(ENV_REPLICATION_FACTOR, Integer.toString(ports.length));
        int replicas = Integer.parseInt(strReplicas);
        if (replicas < 1 || replicas > ports.length) {
            throw new IllegalArgumentException("Invalid replication factor");
        }

        String host = System.getProperty(ENV_CLUSTER_HOST, DEFAULT_HOST);

        return new ClusterConfig(host, ports, replicas);
    }
}
