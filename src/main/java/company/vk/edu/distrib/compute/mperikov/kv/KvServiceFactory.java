package company.vk.edu.distrib.compute.mperikov.kv;

import java.io.IOException;
import java.nio.file.Path;

import company.vk.edu.distrib.compute.AbstractHttpServiceFactory;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.kv.KVService;
import company.vk.edu.distrib.compute.kv.KVServiceTest;

@KVServiceTest
public final class KvServiceFactory extends AbstractHttpServiceFactory<KVService> {
    @Override
    protected KVService doCreate(int port) throws IOException {
        Dao<byte[]> entities = new ByteFileDao(storageDirectory());
        return new KvHttpService(port, entities);
    }

    private static Path storageDirectory() throws IOException {
        String tmpDir = System.getProperty("java.io.tmpdir");
        if (tmpDir == null || tmpDir.isBlank()) {
            throw new IOException("java.io.tmpdir is not set");
        }
        return Path.of(tmpDir, "mperikov-kv");
    }
}
