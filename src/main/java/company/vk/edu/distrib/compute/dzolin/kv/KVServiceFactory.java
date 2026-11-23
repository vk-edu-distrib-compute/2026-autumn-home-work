package company.vk.edu.distrib.compute.dzolin.kv;

import company.vk.edu.distrib.compute.AbstractHttpServiceFactory;

import java.io.IOException;

@company.vk.edu.distrib.compute.kv.KVServiceTest
public class KVServiceFactory extends AbstractHttpServiceFactory<KVServiceImpl> {
    @Override
    protected KVServiceImpl doCreate(int port) throws IOException {
        return new KVServiceImpl(port);
    }
}
