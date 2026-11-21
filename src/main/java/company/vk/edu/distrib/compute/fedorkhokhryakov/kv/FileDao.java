package company.vk.edu.distrib.compute.fedorkhokhryakov.kv;

import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;

public class FileDao implements Dao<byte[]> {
    private static final String TMP_PREFIX = "tmp-";

    private final Path root;
    private final AtomicBoolean closed = new AtomicBoolean();

    public FileDao(Path root) throws IOException {
        this.root = Files.createDirectories(root);
        removeUnfinishedWrites();
    }

    @Override
    public byte[] get(String key) throws IOException {
        try {
            return Files.readAllBytes(pathOf(key));
        } catch (NoSuchFileException e) {
            throw new NoSuchElementException("No value for key: " + key, e);
        }
    }

    @Override
    public void upsert(String key, byte[] value) throws IOException {
        Path target = pathOf(key);
        Path tmp = Files.createTempFile(root, TMP_PREFIX, null);
        try {
            Files.write(tmp, value);
            Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } finally {
            Files.deleteIfExists(tmp);
        }
    }

    @Override
    public void delete(String key) throws IOException {
        Files.deleteIfExists(pathOf(key));
    }

    @Override
    public void close() {
        closed.set(true);
    }

    private Path pathOf(String key) {
        if (closed.get()) {
            throw new IllegalStateException("Dao is closed");
        }
        if (key.isEmpty()) {
            throw new IllegalArgumentException("Key must not be empty");
        }
        return root.resolve(hash(key));
    }

    private static String hash(String key) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(digest.digest(key.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is not available", e);
        }
    }

    private void removeUnfinishedWrites() throws IOException {
        try (DirectoryStream<Path> leftovers = Files.newDirectoryStream(root, TMP_PREFIX + "*")) {
            for (Path leftover : leftovers) {
                Files.deleteIfExists(leftover);
            }
        }
    }
}
