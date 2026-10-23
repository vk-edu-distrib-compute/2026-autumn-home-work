package company.vk.edu.distrib.compute.nickmish.kv;

import company.vk.edu.distrib.compute.AbstractHttpServiceFactory;
import company.vk.edu.distrib.compute.kv.KVService;
import company.vk.edu.distrib.compute.kv.KVServiceTest;
import org.jspecify.annotations.NullMarked;

@KVServiceTest
@NullMarked
public final class KVServiceFactory extends AbstractHttpServiceFactory<KVService> {
    @Override
    protected KVService doCreate(int port) {
        return new KVServiceImpl(port);
    }
}
