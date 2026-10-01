package company.vk.edu.distrib.compute.mrglaster.urlshortener.dao;

import company.vk.edu.distrib.compute.Dao;

import java.io.EOFException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

public class PersistentDao implements Dao<String> {

    private final Map<String, String> storage = new ConcurrentHashMap<>();

    private final Set<String> modifiedKeys = ConcurrentHashMap.newKeySet();
    private final Set<String> removedKeys = ConcurrentHashMap.newKeySet();

    private final ReentrantLock lock = new ReentrantLock();
    private final RandomAccessFile log;

    public PersistentDao(String filePath) throws IOException {
        this.log = new RandomAccessFile(filePath, "rw");
        loadFromFile();
    }

    private void loadFromFile() throws IOException {
        log.seek(0);
        while (log.getFilePointer() < log.length()) {
            try {
                String operation = log.readUTF();
                String key = log.readUTF();

                if ("PUT".equals(operation)) {
                    String value = log.readUTF();
                    storage.put(key, value);
                } else if ("DELETE".equals(operation)) {
                    storage.remove(key);
                }
            } catch (EOFException e) {
                break;
            }
        }
    }

    @Override
    public String get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }
        String value = storage.get(key);
        if (value == null) {
            throw new NoSuchElementException("Key not found: " + key);
        }
        return value;
    }

    @Override
    public void upsert(String key, String value) throws IllegalArgumentException, IOException {
        if (key == null || value == null) {
            throw new IllegalArgumentException("Key and value cannot be null");
        }
        lock.lock();
        try {
            storage.put(key, value);
            modifiedKeys.add(key);
            removedKeys.remove(key);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }
        lock.lock();
        try {
            storage.remove(key);
            removedKeys.add(key);
            modifiedKeys.remove(key);
        } finally {
            lock.unlock();
        }
    }

    public void save() throws IOException {
        lock.lock();
        try {
            log.seek(log.length());
            for (String key : modifiedKeys) {
                log.writeUTF("PUT");
                log.writeUTF(key);
                log.writeUTF(storage.get(key));
            }
            for (String key : removedKeys) {
                log.writeUTF("DELETE");
                log.writeUTF(key);
            }
            modifiedKeys.clear();
            removedKeys.clear();

        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() throws IOException {
        save();
        lock.lock();
        try {
            if (log != null) {
                log.close();
            }
        } finally {
            lock.unlock();
        }
    }
}
