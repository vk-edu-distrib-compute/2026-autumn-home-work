package company.vk.edu.distrib.compute.k0oshara.urlshortener;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Properties;

import company.vk.edu.distrib.compute.Dao;

final class PersistentDao implements Dao<String> {
    private final Path file;
    private Properties values;

    PersistentDao(Path file) throws IOException {
        this.file = file.toAbsolutePath().normalize();
        Files.createDirectories(this.file.getParent());
        values = new Properties();
        if (Files.exists(this.file)) {
            try (InputStream input = Files.newInputStream(this.file)) {
                values.loadFromXML(input);
            } catch (RuntimeException exception) {
                throw new IOException("Invalid DAO file", exception);
            }
        }
    }

    @Override
    public String get(String key) {
        checkKey(key);
        String value = values.getProperty(key);
        if (value == null) {
            throw new NoSuchElementException(key);
        }
        return value;
    }

    @Override
    public void upsert(String key, String value) throws IOException {
        checkKey(key);
        if (value == null) {
            throw new IllegalArgumentException("Value must not be null");
        }
        Properties updated = (Properties) values.clone();
        updated.setProperty(key, value);
        save(updated);
        values = updated;
    }

    @Override
    public void delete(String key) throws IOException {
        checkKey(key);
        if (!values.containsKey(key)) {
            return;
        }
        Properties updated = (Properties) values.clone();
        updated.remove(key);
        save(updated);
        values = updated;
    }

    @Override
    public void close() {
        values.clear();
    }

    private void save(Properties updated) throws IOException {
        try (OutputStream output = Files.newOutputStream(file)) {
            updated.storeToXML(output, null, StandardCharsets.UTF_8);
        }
    }

    private static void checkKey(String key) {
        if (Objects.toString(key, "").isEmpty()) {
            throw new IllegalArgumentException("Key must not be empty");
        }
    }
}
