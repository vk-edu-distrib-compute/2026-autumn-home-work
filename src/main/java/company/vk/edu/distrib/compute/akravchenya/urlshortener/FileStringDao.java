package company.vk.edu.distrib.compute.akravchenya.urlshortener;

import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Base64;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Реализация {@link Dao}, которая хранит все состояние в памяти и отражает каждое
 * изменение в файле снапшотов на диске, чтобы данные сохранялись при перезапуске процесса.
 *
 * <p>При создании снапшот воспроизводится в памяти, после этого при каждой мутации весь файл атомарно перезаписывается.
 */
public class FileStringDao implements Dao<String> {

    private static final String FIELD_SEPARATOR = "\t";

    private final Path file;
    private final Map<String, String> storage = new ConcurrentHashMap<>();
    private final ReentrantLock writeLock = new ReentrantLock();

    FileStringDao(Path file) throws IOException {
        this.file = file.toAbsolutePath();
        var parent = this.file.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        if (Files.isRegularFile(this.file)) {
            loadSnapshot();
        }
    }

    @Override
    public String get(String key) throws NoSuchElementException {
        var value = storage.get(key);
        if (value == null) {
            throw new NoSuchElementException("no value stored for key: " + key);
        }
        return value;
    }

    @Override
    public void upsert(String key, String value) throws IOException {
        writeLock.lock();
        try {
            storage.put(key, value);
            saveSnapshot();
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void delete(String key) throws IOException {
        writeLock.lock();
        try {
            storage.remove(key);
            saveSnapshot();
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void close() throws IOException {
        // State is always flushed on the snapshot write, so nothing to release here.
        Files.deleteIfExists(file);
    }

    private void loadSnapshot() throws IOException {
        Map<String, String> restored = new ConcurrentHashMap<>();
        try (var reader = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
            while (true) {
                var line = reader.readLine();
                if (line == null) {
                    break;
                }
                var separator = line.indexOf(FIELD_SEPARATOR);
                if (separator < 0) {
                    continue;
                }
                var key = decode(line.substring(0, separator));
                var value = decode(line.substring(separator + 1));
                if (value.isEmpty()) {
                    restored.remove(key);
                } else {
                    restored.put(key, value);
                }
            }
        }
        storage.putAll(restored);
    }

    private void saveSnapshot() throws IOException {
        var directory = file.getParent();
        var temp = directory == null
            ? Files.createTempFile("urlshortener-", ".tmp")
            : Files.createTempFile(directory, "urlshortener-", ".tmp");
        try {
            try (var writer = Files.newBufferedWriter(temp, StandardCharsets.UTF_8)) {
                for (var entry : storage.entrySet()) {
                    writer.write(encode(entry.getKey()));
                    writer.write(FIELD_SEPARATOR);
                    writer.write(encode(entry.getValue()));
                    writer.newLine();
                }
            }
            Files.move(temp, file, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } finally {
            Files.deleteIfExists(temp);
        }
    }

    private static String encode(String value) {
        return Base64.getEncoder().encodeToString(value.getBytes(StandardCharsets.UTF_8));
    }

    private static String decode(String value) {
        return new String(Base64.getDecoder().decode(value), StandardCharsets.UTF_8);
    }
}
