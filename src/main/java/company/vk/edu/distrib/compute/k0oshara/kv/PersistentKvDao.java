package company.vk.edu.distrib.compute.k0oshara.kv;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

import company.vk.edu.distrib.compute.Dao;

final class PersistentKvDao implements Dao<byte[]> {
    private static final int UPSERT = 1;
    private static final int DELETE = 2;
    private static final int MIN_KEY_LENGTH = 1;

    private final Path file;
    private final Map<String, byte[]> values = new ConcurrentHashMap<>();
    private final ReentrantLock lock = new ReentrantLock();

    PersistentKvDao(Path file) throws IOException {
        this.file = file.toAbsolutePath().normalize();
        Path parent = this.file.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        if (Files.exists(this.file)) {
            load();
        } else {
            Files.createFile(this.file);
        }
    }

    @Override
    public byte[] get(String key) throws NoSuchElementException, IllegalArgumentException {
        checkKey(key);
        lock.lock();
        try {
            byte[] value = values.get(key);
            if (value == null) {
                throw new NoSuchElementException(key);
            }
            return value.clone();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void upsert(String key, byte[] value) throws IllegalArgumentException, IOException {
        checkKey(key);
        if (value == null) {
            throw new IllegalArgumentException("Value must not be null");
        }
        lock.lock();
        try {
            append(UPSERT, key, value);
            values.put(key, value.clone());
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        checkKey(key);
        lock.lock();
        try {
            if (values.containsKey(key)) {
                append(DELETE, key, null);
                values.remove(key);
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() {
        lock.lock();
        try {
            values.clear();
        } finally {
            lock.unlock();
        }
    }

    private void checkKey(String key) {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("Key must not be empty");
        }
    }

    private void load() throws IOException {
        try (DataInputStream input = new DataInputStream(Files.newInputStream(file))) {
            while (input.available() > 0) {
                int operation = input.readInt();
                String key = readString(input);
                int valueLength = input.readInt();
                if (operation == UPSERT) {
                    if (valueLength < 0) {
                        throw new IOException("Invalid value length");
                    }
                    values.put(key, readBytes(input, valueLength));
                } else if (operation == DELETE && valueLength == -1) {
                    values.remove(key);
                } else {
                    throw new IOException("Invalid journal operation");
                }
            }
        }
    }

    private String readString(DataInputStream input) throws IOException {
        int length = input.readInt();
        if (length < MIN_KEY_LENGTH) {
            throw new IOException("Invalid key length");
        }
        return new String(readBytes(input, length), StandardCharsets.UTF_8);
    }

    private byte[] readBytes(DataInputStream input, int length) throws IOException {
        if (length < 0) {
            throw new IOException("Invalid record length");
        }
        byte[] bytes = input.readNBytes(length);
        if (bytes.length != length) {
            throw new IOException("Incomplete journal record");
        }
        return bytes;
    }

    private void append(int operation, String key, byte[] value) throws IOException {
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        try (DataOutputStream output = new DataOutputStream(Files.newOutputStream(file,
            StandardOpenOption.CREATE, StandardOpenOption.APPEND))) {
            output.writeInt(operation);
            output.writeInt(keyBytes.length);
            output.write(keyBytes);
            output.writeInt(value == null ? -1 : value.length);
            if (value != null) {
                output.write(value);
            }
        }
    }
}
