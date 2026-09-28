package company.vk.edu.distrib.compute.vagifbaratov.urlshortener.dao;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import java.util.concurrent.ConcurrentMap;

public final class PersistentDaoUtils {

    private PersistentDaoUtils() {
    }

    public static void loadStorage(
            Path filepath,
            ConcurrentMap<String, String> storage
    ) throws IOException {
        if (!Files.exists(filepath)) {
            return;
        }

        final var properties = new Properties();

        try (var reader = Files.newBufferedReader(filepath)) {
            properties.load(reader);
        }

        for (String key : properties.stringPropertyNames()) {
            storage.put(key, properties.getProperty(key));
        }
    }

    public static void storeStorage(
            Path filepath,
            ConcurrentMap<String, String> storage
    ) throws IOException {
        Properties properties = new Properties();

        properties.putAll(storage);

        try (var writer = Files.newBufferedWriter(filepath)) {
            properties.store(writer, "Links storage");
        }
    }
}
