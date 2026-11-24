package company.vk.edu.distrib.compute.fedorkhokhryakov.kv;

import company.vk.edu.distrib.compute.AbstractHttpServiceFactory;
import company.vk.edu.distrib.compute.kv.KVService;
import company.vk.edu.distrib.compute.kv.ReplicatedKVServiceTest;

import java.io.IOException;
import java.util.concurrent.Executors;

@ReplicatedKVServiceTest
public class ReplicatedKVServiceFactory extends AbstractHttpServiceFactory<KVService> {
    private static final String REPLICATION_FACTOR_ENV = "KV_REPLICATION_FACTOR";

    @Override
    protected KVService doCreate(int port) throws IOException {
        FileDao dao = new FileDao(KVServiceFactory.storage(port));
        ReplicatingEntityHandler handler = new ReplicatingEntityHandler(port, dao, replicationFactor());
        return new KVServiceImpl(port, handler, Executors.newVirtualThreadPerTaskExecutor());
    }

    private static int replicationFactor() {
        String value = System.getenv(REPLICATION_FACTOR_ENV);
        if (value == null || value.isBlank()) {
            return 0;
        }
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(REPLICATION_FACTOR_ENV + " must be a number: " + value, e);
        }
    }
}
