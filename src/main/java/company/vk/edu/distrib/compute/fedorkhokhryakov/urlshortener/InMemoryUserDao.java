package company.vk.edu.distrib.compute.fedorkhokhryakov.urlshortener;

import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;
import java.util.NoSuchElementException;

public class InMemoryUserDao implements Dao<String> {
    private final Map<String, String> users = new ConcurrentHashMap<>();

    @Override
    public String get(String key) throws NoSuchElementException {
        String password = users.get(key);

        if (password == null) {
            throw new NoSuchElementException("User not found: " + key);
        }

        return password;
    }

    @Override
    public void upsert(String key, String value) {
        users.put(key, value);
    }

    @Override
    public void delete(String key) {
        users.remove(key);
    }

    @Override
    public void close() throws IOException {
        users.clear();
    }
}
