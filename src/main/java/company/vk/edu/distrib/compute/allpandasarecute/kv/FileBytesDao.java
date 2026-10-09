package company.vk.edu.distrib.compute.allpandasarecute.kv;

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.NoSuchElementException;
import java.util.concurrent.locks.ReentrantLock;

import company.vk.edu.distrib.compute.Dao;

public class FileBytesDao implements Dao<byte[]> {
    private final ReentrantLock lock = new ReentrantLock();
    private final Path directory;

    public FileBytesDao(Path directory) throws IOException {
        Files.createDirectories(directory);
        this.directory = directory;
    }

    @Override
    public byte[] get(String key) throws NoSuchElementException, IOException {
        Path file = fileFor(key);
        if (!Files.exists(file)) {
            throw new NoSuchElementException("No value for key '%s'".formatted(key));
        }
        return Files.readAllBytes(file);
    }

    @Override
    public void upsert(String key, byte[] value) throws IOException {
        lock.lock();
        try {
            Files.write(fileFor(key), value);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void delete(String key) throws IOException {
        lock.lock();
        try {
            Files.deleteIfExists(fileFor(key));
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() {
        //
    }

    private Path fileFor(String key) {
        if (key.isBlank()) {
            throw new IllegalArgumentException("Key must not be blank");
        }
        return directory.resolve(URLEncoder.encode(key, StandardCharsets.UTF_8));
    }
}
