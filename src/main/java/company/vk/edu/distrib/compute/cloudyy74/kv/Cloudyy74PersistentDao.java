package company.vk.edu.distrib.compute.cloudyy74.kv;

import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Base64;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class Cloudyy74PersistentDao implements Dao<byte[]> {
    private static final int MIN_OPERATION_PARTS = 2;
    private static final int UPSERT_OPERATION_PARTS = 3;

    private final Map<String, byte[]> storage = new ConcurrentHashMap<>();
    private final Lock writeLock = new ReentrantLock();
    private final Path logFile;

    public Cloudyy74PersistentDao(String logFile) throws IOException {
        this.logFile = Path.of(logFile).toAbsolutePath();

        Path parent = this.logFile.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }

        load();
    }

    @Override
    public byte[] get(String key) throws NoSuchElementException, IllegalArgumentException {
        validateKey(key);
        final var value = storage.get(key);
        if (value == null) {
            throw new NoSuchElementException("no value for key: " + key);
        }

        return value.clone();
    }

    @Override
    public void upsert(String key, byte[] value) throws IllegalArgumentException, IOException {
        validateKey(key);
        final var copy = value.clone();
        writeLock.lock();
        try {
            append("PUT " + encodeKey(key) + " " + Base64.getEncoder().encodeToString(copy));
            storage.put(key, copy);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        validateKey(key);
        writeLock.lock();
        try {
            append("DEL " + encodeKey(key));
            storage.remove(key);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void close() {
        // no resources to release
    }

    public boolean isHealthy() {
        if (Files.exists(logFile)) {
            return Files.isRegularFile(logFile) && Files.isReadable(logFile) && Files.isWritable(logFile);
        }
        final var parent = logFile.getParent();
        return parent != null && Files.isDirectory(parent) && Files.isWritable(parent);
    }

    private static void validateKey(String key) {
        if (key.isEmpty()) {
            throw new IllegalArgumentException("Empty key");
        }
    }

    private static String encodeKey(String key) {
        return Base64.getEncoder().encodeToString(key.getBytes(StandardCharsets.UTF_8));
    }

    private void append(String operation) throws IOException {
        Files.writeString(
                logFile,
                operation + System.lineSeparator(),
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE,
                StandardOpenOption.APPEND
        );
    }

    private void load() throws IOException {
        if (!Files.exists(logFile)) {
            return;
        }

        try (var reader = Files.newBufferedReader(logFile, StandardCharsets.UTF_8)) {
            String line = reader.readLine();
            while (line != null) {
                loadOperation(line);
                line = reader.readLine();
            }
        }
    }

    private void loadOperation(String line) throws IOException {
        String[] parts = line.split(" ", UPSERT_OPERATION_PARTS);
        try {
            if (parts.length < MIN_OPERATION_PARTS) {
                throw new IOException("Missing key in log operation");
            }
            final var key = new String(Base64.getDecoder().decode(parts[1]), StandardCharsets.UTF_8);
            validateKey(key);
            switch (parts[0]) {
                case "PUT" -> {
                    if (parts.length != UPSERT_OPERATION_PARTS) {
                        throw new IOException("Missing value in log operation");
                    }
                    storage.put(key, Base64.getDecoder().decode(parts[2]));
                }
                case "DEL" -> storage.remove(key);
                default -> throw new IOException("Unknown log operation: " + line);
            }
        } catch (IllegalArgumentException e) {
            throw new IOException("Invalid log operation: " + line, e);
        }
    }
}
