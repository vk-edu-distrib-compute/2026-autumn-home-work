package company.vk.edu.distrib.compute.iizhukov.kv;

import java.io.IOException;

import company.vk.edu.distrib.compute.AbstractHttpServiceFactory;
import company.vk.edu.distrib.compute.kv.KVServiceTest;

@KVServiceTest
public final class ApplicationFactory extends AbstractHttpServiceFactory<Application> {
    @Override
    protected Application doCreate(int port) throws IOException {
        return new Application(port);
    }
}
