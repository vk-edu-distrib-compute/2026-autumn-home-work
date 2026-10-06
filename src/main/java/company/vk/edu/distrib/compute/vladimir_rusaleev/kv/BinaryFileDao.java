package company.vk.edu.distrib.compute.vladimir_rusaleev.kv;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.HexFormat;
import java.util.NoSuchElementException;

import company.vk.edu.distrib.compute.Dao;

final class BinaryFileDao implements Dao<byte[]> {
    private static final int PATH_PART_LENGTH = 120;

    private final Path directory;

    BinaryFileDao(Path directory) throws IOException {
        this.directory = directory;
        Files.createDirectories(directory);
    }

    @Override
    public synchronized byte[] get(String key) throws IOException {
        try {
            return Files.readAllBytes(pathFor(key));
        } catch (NoSuchFileException exception) {
            throw new NoSuchElementException(key, exception);
        }
    }

    @Override
    public synchronized void upsert(String key, byte[] value) throws IOException {
        if (value == null) {
            throw new IllegalArgumentException("Null value");
        }
        Path target = pathFor(key);
        Files.createDirectories(target.getParent());
        Files.write(target, value);
    }

    @Override
    public synchronized void delete(String key) throws IOException {
        Files.deleteIfExists(pathFor(key));
    }

    @Override
    public void close() {
        //
    }

    private Path pathFor(String key) {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("Empty key");
        }
        String encoded = HexFormat.of().formatHex(key.getBytes(StandardCharsets.UTF_8));
        Path result = directory;
        for (int offset = 0; offset < encoded.length(); offset += PATH_PART_LENGTH) {
            int end = Math.min(offset + PATH_PART_LENGTH, encoded.length());
            result = result.resolve(encoded.substring(offset, end));
        }
        return result.resolve("value.bin");
    }
}
