package company.vk.edu.distrib.compute.flighen.kv;

import company.vk.edu.distrib.compute.Dao;
import org.jspecify.annotations.NonNull;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class BytesDao implements Dao<byte[]> {
    private final Path filePath;

    private final Map<String, byte[]> db;

    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Lock writeLock = lock.writeLock();

    public BytesDao(Path filePath) throws IOException {
        this.filePath = filePath;
        db = new ConcurrentHashMap<>();

        if (!Files.exists(filePath)) {
            Files.createFile(filePath);
        }

        readFile();
    }

    @Override
    public byte[] get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        if (key == null || key.isBlank()) {
            throw new EmptyKeyException();
        }

        byte[] value = db.get(key);

        if (value == null) {
            throw new NoSuchElementException("db does not contain this key: %s".formatted(key));
        }

        return value;
    }

    @Override
    public void upsert(String key, byte @NonNull [] value) throws IllegalArgumentException, IOException {
        if (key == null || key.isBlank()) {
            throw new EmptyKeyException();
        }

        db.put(key, value);
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        if (key == null || key.isBlank()) {
            throw new EmptyKeyException();
        }

        db.remove(key);
    }

    @Override
    public void close() throws IOException {
        writeLock.lock();
        try (DataOutputStream out = new DataOutputStream(Files.newOutputStream(filePath))) {
            for (Map.Entry<String, byte[]> entry : db.entrySet()) {
                byte[] keyBytes = entry.getKey()
                        .getBytes(StandardCharsets.UTF_8);

                byte[] value = entry.getValue();

                out.writeInt(keyBytes.length);
                out.write(keyBytes);

                out.writeInt(value.length);
                out.write(value);
            }
        } finally {
            writeLock.unlock();
        }
    }

    private void readFile() throws IOException {
        try (DataInputStream in = new DataInputStream(Files.newInputStream(filePath))) {
            while (in.available() > 0) {
                int keyLength = in.readInt();
                byte[] keyBytes = in.readNBytes(keyLength);

                int valueLength = in.readInt();
                byte[] value = in.readNBytes(valueLength);

                String key = decodeKey(keyBytes);

                db.put(key, value);
            }
        }
    }

    private static String decodeKey(byte[] keyBytes) {
        return new String(keyBytes, StandardCharsets.UTF_8);
    }
}
