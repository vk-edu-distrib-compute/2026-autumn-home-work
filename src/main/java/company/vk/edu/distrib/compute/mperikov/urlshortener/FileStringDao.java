package company.vk.edu.distrib.compute.mperikov.urlshortener;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import company.vk.edu.distrib.compute.Dao;

public final class FileStringDao implements Dao<String> {
    private final Path file;
    private final Map<String, String> entries = new ConcurrentHashMap<>();

    public FileStringDao(Path storageFile) throws IOException {
        file = storageFile.toAbsolutePath().normalize();
        load();
    }

    @Override
    public String get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        checkKey(key);
        String stored = entries.get(key);
        if (stored == null) {
            throw new NoSuchElementException("Key is not stored");
        }
        return stored;
    }

    @Override
    public void upsert(String key, String value) throws IllegalArgumentException, IOException {
        checkKey(key);
        entries.put(key, value);
        save();
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        checkKey(key);
        if (entries.remove(key) != null) {
            save();
        }
    }

    @Override
    public void close() {
        entries.clear();
    }

    private static void checkKey(String key) {
        if (key.isEmpty()) {
            throw new IllegalArgumentException("Key must not be empty");
        }
    }

    private void load() throws IOException {
        if (!Files.isRegularFile(file)) {
            return;
        }
        Properties properties = new Properties();
        try (InputStream input = Files.newInputStream(file)) {
            properties.load(input);
        }
        for (String name : properties.stringPropertyNames()) {
            String value = properties.getProperty(name);
            if (value != null) {
                entries.put(name, value);
            }
        }
    }

    private void save() throws IOException {
        Path parent = file.getParent();
        if (parent == null) {
            throw new IOException("Storage path has no parent directory");
        }
        Files.createDirectories(parent);
        Path temporary = Files.createTempFile(parent, ".dao-", ".tmp");
        try {
            writeProperties(temporary);
            moveIntoPlace(temporary, file);
        } catch (IOException ex) {
            deleteQuietly(temporary, ex);
            throw ex;
        }
    }

    private void writeProperties(Path target) throws IOException {
        Properties properties = new Properties();
        properties.putAll(entries);
        try (OutputStream output = Files.newOutputStream(target)) {
            properties.store(output, "mperikov-urlshortener");
        }
    }

    private static void moveIntoPlace(Path temporary, Path target) throws IOException {
        try {
            Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (AtomicMoveNotSupportedException ex) {
            Files.move(temporary, target, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    private static void deleteQuietly(Path temporary, IOException failure) {
        try {
            Files.deleteIfExists(temporary);
        } catch (IOException ex) {
            failure.addSuppressed(ex);
        }
    }
}
