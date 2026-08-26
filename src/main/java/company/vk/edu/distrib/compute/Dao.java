package company.vk.edu.distrib.compute;

import java.io.Closeable;
import java.io.IOException;
import java.util.NoSuchElementException;

public interface Dao<T> extends Closeable {
    T get(String key) throws NoSuchElementException, IllegalArgumentException, IOException;

    void upsert(String key, T value) throws IllegalArgumentException, IOException;

    void delete(String key) throws IllegalArgumentException, IOException;
}
