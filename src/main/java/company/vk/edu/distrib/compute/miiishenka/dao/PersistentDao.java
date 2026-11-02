package company.vk.edu.distrib.compute.miiishenka.dao;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Base64;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import company.vk.edu.distrib.compute.Dao;

public class PersistentDao<T> implements Dao<T> {
    private final String fileName;
    private final Map<String, T> storage;
    private final Function<T, String> serialize;

    public PersistentDao(
            String fileName,
            Function<T, String> serialize,
            Function<String, T> deserialize
    ) throws IOException {
        this.fileName = fileName;
        this.serialize = serialize;
        this.storage = new ConcurrentHashMap<>();
        try (RandomAccessFile file = new RandomAccessFile(fileName, "rw")) {
            if (file.length() == 0) {
                return;
            }
            int size = file.readInt();
            for (int i = 0; i < size; i++) {
                storage.put(file.readUTF(), deserialize.apply(file.readUTF()));
            }
        }
    }

    public static PersistentDao<String> stringDao(String fileName) throws IOException {
        return new PersistentDao<>(
                fileName,
                Function.identity(),
                Function.identity()
        );
    }

    public static PersistentDao<byte[]> byteArrayDao(String fileName) throws IOException {
        return new PersistentDao<>(
                fileName,
                Base64.getEncoder()::encodeToString,
                Base64.getDecoder()::decode
        );
    }

    @Override
    public T get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        return storage.get(key);
    }

    @Override
    public void upsert(String key, T value) throws IllegalArgumentException, IOException {
        storage.put(key, value);
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        storage.remove(key);
    }

    @Override
    public void close() throws IOException {
        try (RandomAccessFile file = new RandomAccessFile(fileName, "rw")) {
            file.setLength(0);
            file.writeInt(storage.size());
            for (var entry : storage.entrySet()) {
                file.writeUTF(entry.getKey());
                file.writeUTF(serialize.apply(entry.getValue()));
            }
        }
    }
}
