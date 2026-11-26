package company.vk.edu.distrib.compute.akhunzianov.kv;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Base64;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;

import company.vk.edu.distrib.compute.Dao;

public class FileDao implements Dao<byte[]> {

    private static final Base64.Encoder NAMES = Base64.getUrlEncoder().withoutPadding();

    private final Path dir;
    private final AtomicBoolean closed = new AtomicBoolean();

    public FileDao(Path dir) throws IOException {
        this.dir = Files.createDirectories(dir);
    }

    @Override
    public byte[] get(String key) throws IOException {
        try {
            return Files.readAllBytes(fileOf(key));
        } catch (NoSuchFileException e) {
            throw new NoSuchElementException("No such key as '" + key + '\'', e);
        }
    }

    @Override
    public void upsert(String key, byte[] value) throws IOException {
        var target = fileOf(key);
        var tmp = Files.createTempFile(dir, ".", ".tmp");
        try {
            Files.write(tmp, value);
            Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE);
        } finally {
            Files.deleteIfExists(tmp);
        }
    }

    @Override
    public void delete(String key) throws IOException {
        Files.deleteIfExists(fileOf(key));
    }

    @Override
    public void close() {
        closed.set(true);
    }

    private Path fileOf(String key) {
        if (closed.get()) {
            throw new IllegalStateException("Dao is closed");
        }
        if (key.isEmpty()) {
            throw new IllegalArgumentException("Key must not be empty");
        }
        return dir.resolve(NAMES.encodeToString(key.getBytes(StandardCharsets.UTF_8)));
    }
}
