package company.vk.edu.distrib.compute.sghdjsdfhgfj.kv;

import company.vk.edu.distrib.compute.AbstractHttpServiceFactory;
import company.vk.edu.distrib.compute.kv.KVService;
import company.vk.edu.distrib.compute.kv.KVServiceTest;

import java.io.IOException;

@KVServiceTest
public class MyKVServiceFactory extends AbstractHttpServiceFactory<KVService> {
    @Override
    protected KVService doCreate(int port) throws IOException {
        return new MyKVService(port);
    }
}
