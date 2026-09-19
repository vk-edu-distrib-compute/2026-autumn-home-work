package company.vk.edu.distrib.compute.sghdjsdfhgfj.urlshortener;

import company.vk.edu.distrib.compute.Dao;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

public class PersistentDao implements Dao<String> {
    private final Map<String, String> data;
    private final String filename;

    PersistentDao(String filename) throws IOException {
        this.filename = filename;
        data = new HashMap<>();
        read();
    }

    @Override
    public String get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        if (!data.containsKey(key)) {
            throw new NoSuchElementException();
        }
        return data.get(key);
    }

    public boolean containsKey(String key) {
        return data.containsKey(key);
    }

    @Override
    public void upsert(String key, String value) throws IllegalArgumentException, IOException {
        data.put(key, value);
        flush();
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        data.remove(key);
        flush();
    }

    @Override
    public void close() throws IOException {
        flush();
    }

    private void read() throws IOException {
        try (RandomAccessFile file = new RandomAccessFile(filename, "rw")) {
            int size = file.readInt();
            for (int i = 0; i < size; i++) {
                data.put(file.readUTF(), file.readUTF());
            }
        }
    }

    private void flush() throws IOException {
        try (RandomAccessFile file = new RandomAccessFile(filename, "rw")) {
            file.writeInt(data.size());
            for (Map.Entry<String, String> entry : data.entrySet()) {
                file.writeUTF(entry.getKey());
                file.writeUTF(entry.getValue());
            }
        }
    }
}
