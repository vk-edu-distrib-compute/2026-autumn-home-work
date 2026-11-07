package company.vk.edu.distrib.compute.virogg.kv;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import company.vk.edu.distrib.compute.AbstractHttpServiceFactory;
import company.vk.edu.distrib.compute.kv.KVService;
import company.vk.edu.distrib.compute.kv.KVServiceTest;

@KVServiceTest
public final class KVServiceFactoryImpl extends AbstractHttpServiceFactory<KVService> {
    @Override
    protected KVService doCreate(int port) throws IOException {
        Path directory = Path.of(System.getProperty("java.io.tmpdir", "/tmp"), "virogg-kv", "port-" + port);
        Files.createDirectories(directory);
        boolean multithreaded = Boolean.parseBoolean(System.getProperty("virogg.kv.multithreaded", "true"));
        return new KVServiceImpl(port, directory, multithreaded);
    }
}
