package company.vk.edu.distrib.compute.randomrandoms.factory;

import company.vk.edu.distrib.compute.AbstractHttpServiceFactory;
import company.vk.edu.distrib.compute.kv.KVServiceTest;
import company.vk.edu.distrib.compute.randomrandoms.kv.Kvs;

import java.io.IOException;

@KVServiceTest
public class KvsFactory extends AbstractHttpServiceFactory<Kvs> {
    @Override
    protected Kvs doCreate(int port) throws IOException {
        return new Kvs(port);
    }
}
