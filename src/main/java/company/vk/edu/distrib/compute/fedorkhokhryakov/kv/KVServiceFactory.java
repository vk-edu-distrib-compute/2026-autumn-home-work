package company.vk.edu.distrib.compute.fedorkhokhryakov.kv;

import company.vk.edu.distrib.compute.AbstractHttpServiceFactory;
import company.vk.edu.distrib.compute.kv.KVService;
import company.vk.edu.distrib.compute.kv.KVServiceTest;

import java.io.IOException;
import java.nio.file.Path;

@KVServiceTest
public class KVServiceFactory extends AbstractHttpServiceFactory<KVService> {
    private static final String THREADS_ENV = "KV_SERVICE_THREADS";
    private static final String STORAGE_DIR = "fedorkhokhryakov-kv";

    @Override
    protected KVService doCreate(int port) throws IOException {
        Path storage = Path.of(System.getProperty("java.io.tmpdir"), STORAGE_DIR, String.valueOf(port));
        return new KVServiceImpl(port, new FileDao(storage), threads());
    }

    private static int threads() {
        String value = System.getenv(THREADS_ENV);
        if (value == null || value.isBlank()) {
            return Runtime.getRuntime().availableProcessors();
        }
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(THREADS_ENV + " must be a number: " + value, e);
        }
    }
}
