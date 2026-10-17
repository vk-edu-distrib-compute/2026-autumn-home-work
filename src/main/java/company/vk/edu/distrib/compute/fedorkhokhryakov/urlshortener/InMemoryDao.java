package company.vk.edu.distrib.compute.fedorkhokhryakov.urlshortener;

import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class InMemoryDao<T> implements Dao<T> {
    private final ConcurrentMap<String, T> data = new ConcurrentHashMap<>();

    @Override
    public T get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        if (key == null) {
            throw new IllegalArgumentException("Key must not be null");
        }

        T value = data.get(key);
        if (value == null) {
            throw new NoSuchElementException(key);
        }

        return value;
    }

    @Override
    public void upsert(String key, T value) throws IllegalArgumentException, IOException {
        if (key == null || value == null) {
            throw new IllegalArgumentException("Key and value must not be null");
        }

        data.put(key, value);
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        if (key == null) {
            throw new IllegalArgumentException("Key must not be null");
        }

        data.remove(key);
    }

    @Override
    public void close() {
        data.clear();
    }
}
