package company.vk.edu.distrib.compute.netheer.kv;

import company.vk.edu.distrib.compute.AbstractHttpServiceFactory;
import company.vk.edu.distrib.compute.kv.KVService;
import company.vk.edu.distrib.compute.kv.KVServiceTest;

import java.io.IOException;
import java.nio.file.Path;

@KVServiceTest
public final class KVServiceFactory extends AbstractHttpServiceFactory<KVService> {

    @Override
    protected KVService doCreate(int port) throws IOException {
        Path storagePath = Path.of(
                System.getProperty("java.io.tmpdir"),
                "netheer-kv",
                port + ".db"
        );

        PersistentDao dao = new PersistentDao(storagePath);
        return new KVServiceImplementation(port, dao);
    }
}
