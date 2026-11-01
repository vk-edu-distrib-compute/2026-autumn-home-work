package company.vk.edu.distrib.compute.akhunzianov.kv;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import company.vk.edu.distrib.compute.AbstractHttpServiceFactory;
import company.vk.edu.distrib.compute.kv.KVService;
import company.vk.edu.distrib.compute.kv.KVServiceTest;

@KVServiceTest
public class KVServiceFactory extends AbstractHttpServiceFactory<KVService> {

    private static final String STORAGE_DIR = "KV_STORAGE_DIR";
    private static final String THREADS = "KV_THREADS";

    @Override
    protected KVService doCreate(int port) throws IOException {
        return new KVServiceImpl(port, new FileDao(storageDir()), threads());
    }

    private static int threads() {
        var value = System.getenv(THREADS);
        if (value == null || value.isBlank()) {
            return 1;
        }
        return Integer.parseInt(value.strip());
    }

    private static Path storageDir() throws IOException {
        var dir = System.getenv(STORAGE_DIR);
        if (dir == null || dir.isBlank()) {
            return Files.createTempDirectory("kv-");
        }
        return Path.of(dir);
    }
}
