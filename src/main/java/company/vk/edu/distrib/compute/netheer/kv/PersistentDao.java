package company.vk.edu.distrib.compute.netheer.kv;

import company.vk.edu.distrib.compute.Dao;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public final class PersistentDao implements Dao<byte[]> {
    private static final byte UPSERT = 1;
    private static final byte DELETE = 2;
    private static final int END_OF_STREAM = -1;

    private final Path storagePath;
    private final Map<String, byte[]> data = new ConcurrentHashMap<>();
    private final DataOutputStream output;
    private final Lock writeLock = new ReentrantLock();

    public PersistentDao(Path storagePath) throws IOException {
        if (storagePath == null) {
            throw new IllegalArgumentException("Storage path can't be null");
        }
        this.storagePath = storagePath.toAbsolutePath().normalize();
        Path parentDirectory = this.storagePath.getParent();
        Files.createDirectories(parentDirectory);

        if (Files.exists(this.storagePath)) {
            uploadData();
        }

        this.output = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(
                this.storagePath,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.APPEND)));
    }

    @Override
    public byte[] get(String key) throws IOException {
        if (key.isEmpty()) {
            throw new IllegalArgumentException("Key is empty");
        }
        byte[] value = data.get(key);
        if (value == null) {
            throw new NoSuchElementException("Key not found: " + key);
        }

        return value;
    }

    @Override
    public void upsert(String key, byte[] value) throws IOException {
        if (key.isEmpty()) {
            throw new IllegalArgumentException("Key is empty");
        }

        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);

        writeLock.lock();
        try {
            output.writeByte(UPSERT);
            output.writeInt(keyBytes.length);
            output.write(keyBytes);
            output.writeInt(value.length);
            output.write(value);
            output.flush();

            data.put(key, value);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void delete(String key) throws IOException {
        if (key.isEmpty()) {
            throw new IllegalArgumentException("Key is empty");
        }

        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);

        writeLock.lock();
        try {
            output.writeByte(DELETE);
            output.writeInt(keyBytes.length);
            output.write(keyBytes);
            output.flush();

            data.remove(key);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void close() throws IOException {
        writeLock.lock();
        try {
            output.close();
        } finally {
            writeLock.unlock();
        }
    }

    private void uploadData() throws IOException {
        try (DataInputStream input = new DataInputStream(
                new BufferedInputStream(
                Files.newInputStream(this.storagePath)))) {
            int operation = input.read();
            while (operation != END_OF_STREAM) {
                readOperation(input, operation);
                operation = input.read();
            }
        }
    }

    private void readOperation(DataInputStream input, int operation) throws IOException {
        String key = new String(readBytes(input), StandardCharsets.UTF_8);

        switch (operation) {
            case UPSERT -> data.put(key, readBytes(input));
            case DELETE -> data.remove(key);
            default -> throw new IOException("Unknown operation: " + operation);
        }
    }

    private static byte[] readBytes(DataInputStream input) throws IOException {
        int length = input.readInt();
        byte[] bytes = new byte[length];
        input.readFully(bytes);
        return bytes;
    }
}
