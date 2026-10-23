package company.vk.edu.distrib.compute.allpandasarecute.kv;

import java.io.IOException;
import java.nio.file.Path;

import company.vk.edu.distrib.compute.AbstractHttpServiceFactory;
import company.vk.edu.distrib.compute.kv.KVService;
import company.vk.edu.distrib.compute.kv.KVServiceTest;

@KVServiceTest
public class KVServiceFactory extends AbstractHttpServiceFactory<KVService> {
    private static final String STORAGE_ROOT = "allpandasarecute-kv";

    @Override
    protected KVService doCreate(int port) throws IOException {
        Path root = Path.of(System.getProperty("java.io.tmpdir"), STORAGE_ROOT, Integer.toString(port));
        return new KVServiceImpl(port, new FileBytesDao(root.resolve("entities")));
    }
}
