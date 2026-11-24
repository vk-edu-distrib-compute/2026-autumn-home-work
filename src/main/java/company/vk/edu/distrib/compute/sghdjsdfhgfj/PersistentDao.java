package company.vk.edu.distrib.compute.sghdjsdfhgfj;

import company.vk.edu.distrib.compute.Dao;

import java.io.*;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.locks.ReentrantLock;

public class PersistentDao implements Dao<byte[]> {
    private final Map<String, byte[]> data;
    private final String filename;
    private final ReentrantLock lock;
    private static final int MAX_LENGTH = 2048;

    public PersistentDao(Path path) throws IOException {
        this.filename = path.toString();
        data = new HashMap<>();
        lock = new ReentrantLock();
        read();
    }

    @Override
    public byte[] get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        if (!data.containsKey(key)) {
            throw new NoSuchElementException();
        }
        return data.get(key);
    }

    public boolean containsKey(String key) {
        return data.containsKey(key);
    }

    @Override
    public void upsert(String key, byte[] value) throws IllegalArgumentException, IOException {
        if (value.length > MAX_LENGTH) {
            throw new IllegalArgumentException("value is too long");
        }
        try {
            lock.lock();
            data.put(key, value);
            append(key, value);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        try {
            lock.lock();
            data.remove(key);
            append(key, new byte[0]);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() throws IOException {
        //
    }

    private void read() throws IOException {
        try (RandomAccessFile file = new RandomAccessFile(filename, "rw")) {
            byte[] value = new byte[MAX_LENGTH];
            while (true) {
                try {
                    String key = file.readUTF();
                    int valueLength = file.readInt();
                    if (valueLength == 0) {
                        data.remove(key);
                    } else {
                        file.read(value, 0, valueLength);
                        data.put(key, Arrays.copyOfRange(value, 0, valueLength));
                    }
                } catch (EOFException e) {
                    break;
                }
            }
        }
    }

    private void append(String key, byte[] value) throws IOException {
        try (RandomAccessFile file = new RandomAccessFile(filename, "rw")) {
            file.seek(file.length());
            file.writeUTF(key);
            file.writeInt(value.length);
            file.write(value);
        }
    }
}
