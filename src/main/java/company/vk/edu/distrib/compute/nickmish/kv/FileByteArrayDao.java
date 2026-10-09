package company.vk.edu.distrib.compute.nickmish.kv;

import company.vk.edu.distrib.compute.Dao;
import org.jspecify.annotations.NullMarked;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.NoSuchElementException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public final class FileByteArrayDao implements Dao<byte[]> {
    private final Path dir;
    private final Lock lock = new ReentrantLock();
    private boolean closed;

    public FileByteArrayDao(Path path) throws IOException {
        this.dir = path;
        Files.createDirectories(path);
    }

    @Override
    @NullMarked
    public byte[] get(String key) throws IOException {
        lock.lock();
        try {
            checkOpen();
            Path file = resolveKeyPath(key);
            if (!Files.exists(file)) {
                throw new NoSuchElementException("No value for key: " + key);
            }
            return Files.readAllBytes(file);
        } finally {
            lock.unlock();
        }
    }

    @Override
    @NullMarked
    public void upsert(String key, byte[] value) throws IOException {
        lock.lock();
        try {
            checkOpen();
            Path target = resolveKeyPath(key);
            Path tmp = Files.createTempFile(dir, null, null);
            try {
                Files.write(tmp, value);
                Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } catch (IOException | RuntimeException e) {
                Files.deleteIfExists(tmp);
                throw e;
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    @NullMarked
    public void delete(String key) throws IOException {
        lock.lock();
        try {
            checkOpen();
            Files.deleteIfExists(resolveKeyPath(key));
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() throws IOException {
        lock.lock();
        try {
            closed = true;
        } finally {
            lock.unlock();
        }
    }

    public boolean isAvailable() {
        lock.lock();
        try {
            return !closed && Files.isDirectory(dir) && Files.isReadable(dir) && Files.isWritable(dir);
        } finally {
            lock.unlock();
        }
    }

    private void checkOpen() throws IOException {
        if (closed) {
            throw new IOException("DAO is closed");
        }
    }

    private Path resolveKeyPath(String key) {
        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 not available", e);
        }
        byte[] hash = digest.digest(key.getBytes(StandardCharsets.UTF_8));
        return dir.resolve(HexFormat.of().formatHex(hash) + ".bin");
    }
}
