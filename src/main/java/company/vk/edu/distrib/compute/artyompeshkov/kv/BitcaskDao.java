package company.vk.edu.distrib.compute.artyompeshkov.kv;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

import company.vk.edu.distrib.compute.Dao;

public class BitcaskDao implements Dao<byte[]> {
    private static final int DELETED = -1;

    private final RandomAccessFile file;
    private final Map<String, Long> index = new ConcurrentHashMap<>();

    public BitcaskDao(Path dir) throws IOException {
        Files.createDirectories(dir);
        this.file = new RandomAccessFile(dir.resolve("file.log").toFile(), "rw");
        loadIndex();
    }

    @Override
    public synchronized byte[] get(String key) throws IOException {
        checkKey(key);
        Long position = index.get(key);
        if (position == null) {
            throw new NoSuchElementException("Key not found: " + key);
        }
        file.seek(position);
        byte[] value = new byte[file.readInt()];
        file.readFully(value);
        return value;
    }

    @Override
    public synchronized void upsert(String key, byte[] value) throws IOException {
        checkKey(key);
        long position = append(key, value.length);
        file.write(value);
        index.put(key, position);
    }

    @Override
    public synchronized void delete(String key) throws IOException {
        checkKey(key);
        if (index.remove(key) != null) {
            append(key, DELETED);
        }
    }

    @Override
    public synchronized void close() throws IOException {
        file.close();
    }

    boolean isAvailable() {
        return file.getChannel().isOpen();
    }

    private void loadIndex() throws IOException {
        while (file.getFilePointer() < file.length()) {
            String key = file.readUTF();
            long position = file.getFilePointer();
            int size = file.readInt();
            if (size == DELETED) {
                index.remove(key);
            } else {
                index.put(key, position);
                file.seek(position + Integer.BYTES + size);
            }
        }
    }

    private long append(String key, int size) throws IOException {
        file.seek(file.length());
        file.writeUTF(key);
        long position = file.getFilePointer();
        file.writeInt(size);
        return position;
    }

    private static void checkKey(String key) {
        if (key.isEmpty()) {
            throw new IllegalArgumentException("Key must not be empty");
        }
    }
}
