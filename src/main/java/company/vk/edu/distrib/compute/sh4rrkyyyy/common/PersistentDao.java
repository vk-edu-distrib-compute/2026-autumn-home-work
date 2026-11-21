package company.vk.edu.distrib.compute.sh4rrkyyyy.common;

import company.vk.edu.distrib.compute.Dao;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

public class PersistentDao<T> implements Dao<T> {
    private final Map<String, T> shortToLongLinks = new ConcurrentHashMap<>();
    private final Serializer<T> serializer;
    private final Path log;
    private final ReentrantLock writeLock = new ReentrantLock();

    public PersistentDao(String file, Serializer<T> serializer) throws IOException {
        this.serializer = serializer;
        this.log = Path.of(System.getProperty("java.io.tmpdir"), file);
        if (!Files.exists(log)) {
            Files.createFile(log);
        }
        log.toFile().deleteOnExit();
        restore();
    }

    @Override
    public T get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        if (key == null) {
            throw new IllegalArgumentException("get: key is null");
        }
        T val = shortToLongLinks.get(key);
        if (val == null) {
            throw new NoSuchElementException("No link for id " + key);
        }
        return val;
    }

    @Override
    public void upsert(String key, T value) throws IllegalArgumentException, IOException {
        if (value == null) {
            throw new IllegalArgumentException("upsert: key or value is null");
        }
        writeLock.lock();
        try {
            shortToLongLinks.put(key, value);
            append(key, value, /*isPut*/ true);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        writeLock.lock();
        try {
            shortToLongLinks.remove(key);
            append(key, null, /*isPut*/ false);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void close() throws IOException {
        // Nothing to do: log file is removed via deleteOnExit
    }

    private void append(String key, T value, boolean isPut) throws IOException {
        try (DataOutputStream out = new DataOutputStream(
                new BufferedOutputStream(
                        Files.newOutputStream(log, StandardOpenOption.APPEND)))) {
            out.writeBoolean(isPut);
            out.writeUTF(key);
            if (isPut) {
                serializer.write(out, value);
            }
        }
    }

    private void restore() throws IOException {
        if (Files.size(log) == 0) {
            return;
        }
        try (DataInputStream in = new DataInputStream(
                new BufferedInputStream(Files.newInputStream(log)))) {
            try {
                while (true) {
                    boolean isPut = in.readBoolean();
                    String key = in.readUTF();
                    if (isPut) {
                        shortToLongLinks.put(key, serializer.read(in));
                    } else {
                        shortToLongLinks.remove(key);
                    }
                }
            } catch (EOFException expected) {
                // end of file
            }
        }
    }
}
