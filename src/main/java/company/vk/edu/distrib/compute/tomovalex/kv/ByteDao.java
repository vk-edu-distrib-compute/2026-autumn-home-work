package company.vk.edu.distrib.compute.tomovalex.kv;

import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.NoSuchElementException;

public class ByteDao implements Dao<byte[]> {
    private final Path directory;

    public ByteDao(Path directory) throws IOException {
        this.directory = directory;
        Files.createDirectories(directory);
    }

    @Override
    public byte[] get(String key) throws IOException {
        Path file = generatePathForKey(key);
        try {
            return Files.readAllBytes(file);
        } catch (NoSuchFileException e) {
            throw new NoSuchElementException(key, e);
        }
    }

    @Override
    public void upsert(String key, byte[] value) throws IOException {
        Path file = generatePathForKey(key);
        if (value == null) {
            throw new IllegalArgumentException();
        }
        Files.write(file, value);
    }

    @Override
    public void delete(String key) throws IOException {
        Files.deleteIfExists(generatePathForKey(key));
    }

    @Override
    public void close() {
        //
    }

    private Path generatePathForKey(String key) {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException();
        }
        return directory.resolve(key);
    }
}
