package company.vk.edu.distrib.compute.randomrandoms.kv;

import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

public class RamDao<T> implements company.vk.edu.distrib.compute.Dao<T> {
    private final Map<String, T> map;

    public RamDao() {
        map = new HashMap<>();
    }

    @Override
    public T get(String key) throws NoSuchElementException, IllegalArgumentException {
        if (map.containsKey(key)) {
            return map.get(key);
        } else {
            throw new NoSuchElementException();
        }
    }

    @Override
    public void upsert(String key, T value) throws IllegalArgumentException {
        map.put(key, value);
    }

    @Override
    public void delete(String key) throws IllegalArgumentException {
        map.remove(key);
    }

    @Override
    public void close() {
        // skibidi dop dop yes yes
    }
}
