package company.vk.edu.distrib.compute.dzolin.kv;

import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

public class PersistentDao implements Dao<byte[]> {
    private final Map<String, byte[]> storage = new ConcurrentHashMap<>();
    private final ReentrantLock lock = new ReentrantLock();
    private final RandomAccessFile log;

    public PersistentDao(String path) throws IOException {
        var file = Path.of(path).toAbsolutePath();
        Files.createDirectories(file.getParent());
        log = new RandomAccessFile(file.toFile(), "rw");
        init();
    }

    private void init() throws IOException {
        while (log.getFilePointer() < log.length()) {
            var key = log.readUTF();
            var length = log.readInt();
            if (length == -1) {
                storage.remove(key);
            } else {
                storage.put(key, readValue(length));
            }
        }
    }

    private byte[] readValue(int length) throws IOException {
        var value = new byte[length];
        log.readFully(value);
        return value;
    }

    @Override
    public byte[] get(String key) {
        var value = storage.get(key);
        if (value == null) {
            throw new NoSuchElementException("no such value for key: " + key);
        }
        return value.clone();
    }

    @Override
    public void upsert(String key, byte[] value) throws IOException {
        lock.lock();
        try {
            append(key, value);
            storage.put(key, value.clone());
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void delete(String key) throws IOException {
        lock.lock();
        try {
            log.writeUTF(key);
            log.writeInt(-1);
            storage.remove(key);
        } finally {
            lock.unlock();
        }
    }

    private void append(String key, byte[] value) throws IOException {
        log.writeUTF(key);
        log.writeInt(value.length);
        log.write(value);
    }

    @Override
    public void close() throws IOException {
        lock.lock();
        try {
            log.close();
        } finally {
            lock.unlock();
        }
    }
}
