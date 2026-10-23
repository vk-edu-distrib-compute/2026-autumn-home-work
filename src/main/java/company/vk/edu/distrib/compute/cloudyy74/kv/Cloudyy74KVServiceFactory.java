package company.vk.edu.distrib.compute.cloudyy74.kv;

import company.vk.edu.distrib.compute.AbstractHttpServiceFactory;
import company.vk.edu.distrib.compute.kv.KVServiceTest;

import java.io.IOException;

@KVServiceTest
public class Cloudyy74KVServiceFactory extends AbstractHttpServiceFactory<Cloudyy74KVService> {
    @Override
    protected Cloudyy74KVService doCreate(int port) throws IOException {
        return new Cloudyy74KVService(port);
    }
}
