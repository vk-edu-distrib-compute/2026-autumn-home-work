package company.vk.edu.distrib.compute.tomovalex.kv;

import company.vk.edu.distrib.compute.AbstractHttpServiceFactory;
import company.vk.edu.distrib.compute.kv.KVServiceTest;
import company.vk.edu.distrib.compute.kv.ReplicatedKVServiceTest;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;

@KVServiceTest
@ReplicatedKVServiceTest
public class KVServiceFactory extends AbstractHttpServiceFactory<KVService> {
    @Override
    protected KVService doCreate(int port) throws IOException {
        Path directory = Path.of(System.getProperty("java.io.tmpdir"), "tomovalex-kv", Integer.toString(port));
        boolean multithreaded = Boolean.parseBoolean(System.getenv("MULTITHREADED"));
        int[] ports = getClusterPorts(port);
        String factor = System.getenv("REPLICATION_FACTOR");
        int replicationFactor = factor == null
            ? (int) Arrays.stream(ports).distinct().count() : Integer.parseInt(factor);
        return new KVService(port, new ByteDao(directory), multithreaded, replicationFactor, ports);
    }

    private static int[] getClusterPorts(int port) {
        String clusterPorts = System.getenv("CLUSTER_PORTS");
        if (clusterPorts == null || clusterPorts.isBlank()) {
            return new int[]{port};
        }
        return Arrays.stream(clusterPorts.split(",", -1)).map(String::trim).mapToInt(Integer::parseInt).toArray();
    }
}
