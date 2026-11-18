package company.vk.edu.distrib.compute.fedorkhokhryakov.kv;

import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class StringBytesDao implements Dao<byte[]> {
    private final Dao<String> delegate;

    public StringBytesDao(Dao<String> delegate) {
        this.delegate = delegate;
    }

    @Override
    public byte[] get(String key) throws IOException {
        return delegate.get(key).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public void upsert(String key, byte[] value) throws IOException {
        delegate.upsert(key, new String(value, StandardCharsets.UTF_8));
    }

    @Override
    public void delete(String key) throws IOException {
        delegate.delete(key);
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }
}
