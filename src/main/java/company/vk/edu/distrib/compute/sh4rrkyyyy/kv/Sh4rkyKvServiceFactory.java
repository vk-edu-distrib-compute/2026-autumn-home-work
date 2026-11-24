package company.vk.edu.distrib.compute.sh4rrkyyyy.kv;

import company.vk.edu.distrib.compute.AbstractHttpServiceFactory;
import company.vk.edu.distrib.compute.kv.KVServiceTest;

import java.io.IOException;

@KVServiceTest
public class Sh4rkyKvServiceFactory extends AbstractHttpServiceFactory<Sh4rkyKvService> {
    @Override
    protected Sh4rkyKvService doCreate(int port) throws IOException {
        return new Sh4rkyKvService(port);
    }
}
