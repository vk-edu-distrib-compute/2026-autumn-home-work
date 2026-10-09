package company.vk.edu.distrib.compute.k0oshara.kv;

import java.io.IOException;
import java.nio.file.Path;

import company.vk.edu.distrib.compute.AbstractHttpServiceFactory;
import company.vk.edu.distrib.compute.kv.KVService;
import company.vk.edu.distrib.compute.kv.KVServiceTest;

@KVServiceTest
public final class KvFactory extends AbstractHttpServiceFactory<KVService> {
    @Override
    protected KVService doCreate(int port) throws IOException {
        String defaultFile = Path.of(System.getProperty("java.io.tmpdir"), "k0oshara-kv",
            "values.log").toString();
        Path file = Path.of(System.getProperty("k0oshara.kv.file", defaultFile));
        return new KvService(port, new PersistentKvDao(file));
    }
}
