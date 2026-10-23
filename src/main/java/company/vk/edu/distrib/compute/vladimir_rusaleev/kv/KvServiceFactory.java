package company.vk.edu.distrib.compute.vladimir_rusaleev.kv;

import java.io.IOException;

import company.vk.edu.distrib.compute.AbstractHttpServiceFactory;
import company.vk.edu.distrib.compute.kv.KVService;
import company.vk.edu.distrib.compute.kv.KVServiceTest;

@KVServiceTest
public final class KvServiceFactory extends AbstractHttpServiceFactory<KVService> {
    @Override
    protected KVService doCreate(int port) throws IOException {
        int threads = Integer.parseInt(System.getenv().getOrDefault("KV_THREADS", "1"));
        return new KvServiceImpl(port, threads);
    }
}
