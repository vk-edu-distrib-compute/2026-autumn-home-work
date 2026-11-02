package company.vk.edu.distrib.compute.flighen.kv;

import company.vk.edu.distrib.compute.AbstractHttpServiceFactory;
import company.vk.edu.distrib.compute.kv.KVServiceTest;
import company.vk.edu.distrib.compute.kv.ReplicatedKVServiceTest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

@KVServiceTest
@ReplicatedKVServiceTest
public class FlighenKVServiceFactory extends AbstractHttpServiceFactory<FlighenKVService> {
    private static final Logger log = LoggerFactory.getLogger(FlighenKVServiceFactory.class);

    private static final int THREADSNUM = 8;
    private static final int MIN_REPLICA_FACTOR = 1;
    private static final String REPLICA_FACTOR_ENV = "KV_REPLICA_FACTOR";

    @Override
    protected FlighenKVService doCreate(int port) throws IOException {
        boolean multithreaded = Boolean.parseBoolean(System.getenv("KV_MULTITHREADED"));

        int threads = 1;

        if (multithreaded) {
            threads = THREADSNUM;
            log.info("Starting KV Service in multithreading mode");
        }

        return new FlighenKVService(port, threads, getReplicaFactor());
    }

    private static int getReplicaFactor() {
        String rawReplicaFactor = System.getenv(REPLICA_FACTOR_ENV);
        if (rawReplicaFactor == null || rawReplicaFactor.isBlank()) {
            return 0;
        }

        int replicaFactor = Integer.parseInt(rawReplicaFactor);
        if (replicaFactor < MIN_REPLICA_FACTOR) {
            throw new IllegalArgumentException("Replica factor must be positive");
        }
        return replicaFactor;
    }
}
