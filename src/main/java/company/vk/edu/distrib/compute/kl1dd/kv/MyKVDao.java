package company.vk.edu.distrib.compute.kl1dd.kv;

import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.NoSuchElementException;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MyKVDao implements Dao<byte[]> {
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final Path storageDir;

    public MyKVDao(int port) throws IOException {
        storageDir = Path.of(System.getProperty("java.io.tmpdir"), "kl1dd-kv-storage-" + port);
        Files.createDirectories(storageDir);
    }

    @Override
    public byte[] get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        lock.readLock().lock();
        try {
            Path filePath = storageDir.resolve(key);

            if (!Files.exists(filePath)) {
                throw new NoSuchElementException();
            }

            return Files.readAllBytes(filePath);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void upsert(String key, byte[] value) throws IllegalArgumentException, IOException {
        lock.writeLock().lock();
        Path tempFile = null;
        try {
            Path filePath = storageDir.resolve(key);

            tempFile = Files.createTempFile(storageDir, "kv-", ".tmp");
            Files.write(tempFile, value);
            Files.move(tempFile, filePath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } finally {
            try {
                if (tempFile != null) {
                    Files.deleteIfExists(tempFile);
                }
            } finally {
                lock.writeLock().unlock();
            }
        }
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        lock.writeLock().lock();
        try {
            Path filePath = storageDir.resolve(key);
            Files.deleteIfExists(filePath);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void close() throws IOException {
        // later
    }
}
