package company.vk.edu.distrib.compute.mperikov.kv;

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.NoSuchElementException;

import company.vk.edu.distrib.compute.Dao;

final class ByteFileDao implements Dao<byte[]> {
    private static final String FILE_MARK = "k";

    private final Path root;

    ByteFileDao(Path storageRoot) throws IOException {
        root = storageRoot.toAbsolutePath().normalize();
        Files.createDirectories(root);
    }

    @Override
    public byte[] get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        checkKey(key);
        Path stored = file(key);
        if (!Files.isRegularFile(stored)) {
            throw new NoSuchElementException("Key is not stored");
        }
        return Files.readAllBytes(stored);
    }

    @Override
    public void upsert(String key, byte[] value) throws IllegalArgumentException, IOException {
        checkKey(key);
        Files.write(file(key), value);
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        checkKey(key);
        Files.deleteIfExists(file(key));
    }

    @Override
    public void close() {
        // Files stay on disk for the next process.
    }

    private static void checkKey(String key) {
        if (key.isEmpty()) {
            throw new IllegalArgumentException("Key must not be empty");
        }
    }

    private Path file(String key) {
        return root.resolve(FILE_MARK + URLEncoder.encode(key, StandardCharsets.UTF_8));
    }
}
