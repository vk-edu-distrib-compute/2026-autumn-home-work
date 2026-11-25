package company.vk.edu.distrib.compute.randomrandoms.factory;

import company.vk.edu.distrib.compute.AbstractHttpServiceFactory;
import company.vk.edu.distrib.compute.kv.ReplicatedKVServiceTest;
import company.vk.edu.distrib.compute.randomrandoms.kv.ReplKvs;

import java.io.IOException;

@ReplicatedKVServiceTest
public class ReplKvsFactory extends AbstractHttpServiceFactory<ReplKvs> {
    @Override
    protected ReplKvs doCreate(int port) throws IOException {
        return new ReplKvs(port);
    }
}
