package company.vk.edu.distrib.compute.kl1dd.kv;

import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.NoSuchElementException;

public class MyKVDao implements Dao<byte[]> {
    private final Path storageDir;

    public MyKVDao() throws IOException {
        this.storageDir = Files.createTempDirectory("kv-storage");
        Files.createDirectories(storageDir);
    }

    @Override
    public byte[] get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        Path filePath = storageDir.resolve(key);

        if (!Files.exists(filePath)) {
            throw new NoSuchElementException();
        }

        return Files.readAllBytes(filePath);
    }

    @Override
    public void upsert(String key, byte[] value) throws IllegalArgumentException, IOException {
        Path filePath = storageDir.resolve(key);
        Files.write(filePath, value);
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        Path filePath = storageDir.resolve(key);
        Files.deleteIfExists(filePath);
    }

    @Override
    public void close() throws IOException {
        // later
    }
}
