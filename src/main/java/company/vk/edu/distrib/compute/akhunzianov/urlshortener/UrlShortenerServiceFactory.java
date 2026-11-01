package company.vk.edu.distrib.compute.akhunzianov.urlshortener;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

import company.vk.edu.distrib.compute.AbstractHttpServiceFactory;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.akhunzianov.InMemoryDao;
import company.vk.edu.distrib.compute.akhunzianov.PersistentDao;
import company.vk.edu.distrib.compute.akhunzianov.kv.ClusterDao;
import company.vk.edu.distrib.compute.akhunzianov.kv.RemoteDao;
import company.vk.edu.distrib.compute.urlshortener.UrlShortenerService;
import company.vk.edu.distrib.compute.urlshortener.UrlShortenerTest;

@UrlShortenerTest
public class UrlShortenerServiceFactory extends AbstractHttpServiceFactory<UrlShortenerService> {

    static final String STORAGE_DIR = "SHORTENER_STORAGE_DIR";
    static final String KV_PORTS = "SHORTENER_KV_PORTS";
    private static final String PORT_SEPARATOR = ",";

    @Override
    protected UrlShortenerService doCreate(int port) throws IOException {
        return new UrlShortenerServiceImpl(port, links(), null);
    }

    static Dao<String> links() throws IOException {
        var value = System.getenv(KV_PORTS);
        if (value == null || value.isBlank()) {
            return storage("links");
        }
        var ports = Arrays.stream(value.split(PORT_SEPARATOR))
            .map(String::strip)
            .mapToInt(Integer::parseInt)
            .toArray();
        return ports.length == 1 ? new RemoteDao(ports[0]) : new ClusterDao(ports);
    }

    static Dao<String> storage(String name) throws IOException {
        var dir = System.getenv(STORAGE_DIR);
        if (dir == null || dir.isBlank()) {
            return new InMemoryDao();
        }
        var directory = Path.of(dir);
        Files.createDirectories(directory);
        return new PersistentDao(directory.resolve(name + ".log"));
    }
}
