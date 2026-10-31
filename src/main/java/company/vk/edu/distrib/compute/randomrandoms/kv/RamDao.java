package company.vk.edu.distrib.compute.randomrandoms.kv;

import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

public class RamDao<T> implements company.vk.edu.distrib.compute.Dao<T> {
    private final Map<String, T> map;

    public RamDao() {
        map = new ConcurrentHashMap<>();
    }

    @Override
    public T get(String key) throws NoSuchElementException, IllegalArgumentException {
        var val = map.get(key);
        if (val == null) {
            throw new NoSuchElementException();
        } else {
            return val;
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
