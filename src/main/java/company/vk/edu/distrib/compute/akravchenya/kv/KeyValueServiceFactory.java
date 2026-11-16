package company.vk.edu.distrib.compute.akravchenya.kv;

import company.vk.edu.distrib.compute.AbstractHttpServiceFactory;
import company.vk.edu.distrib.compute.kv.KVServiceTest;

import java.io.IOException;

/**
 * Создает персонализированный {@link KeyValueServiceImpl}.
 */
@KVServiceTest
public class KeyValueServiceFactory extends AbstractHttpServiceFactory<KeyValueServiceImpl> {

    @Override
    protected KeyValueServiceImpl doCreate(int port) throws IOException {
        return new KeyValueServiceImpl(port);
    }
}
