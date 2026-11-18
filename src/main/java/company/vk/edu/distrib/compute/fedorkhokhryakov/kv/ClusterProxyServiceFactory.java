package company.vk.edu.distrib.compute.fedorkhokhryakov.kv;

import company.vk.edu.distrib.compute.AbstractHttpServiceFactory;
import company.vk.edu.distrib.compute.kv.KVService;

import java.io.IOException;
import java.util.Arrays;

public class ClusterProxyServiceFactory extends AbstractHttpServiceFactory<KVService> {
    private static final String PORTS_ENV = "CLUSTER_PORTS";

    @Override
    protected KVService doCreate(int port) throws IOException {
        StringBytesDao dao = new StringBytesDao(new ClusterRemoteDaoFactory().create(clusterPorts()));
        return new KVServiceImpl(port, dao, Runtime.getRuntime().availableProcessors());
    }

    private static int[] clusterPorts() {
        String value = System.getenv(PORTS_ENV);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(PORTS_ENV + " must list KV node ports, e.g. 18091,18092,18093");
        }
        return Arrays.stream(value.split(","))
            .map(String::trim)
            .mapToInt(Integer::parseInt)
            .toArray();
    }
}
