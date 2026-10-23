package company.vk.edu.distrib.compute.akravchenya.kv;

import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Base64;
import java.util.NoSuchElementException;

/**
 * Реализация {@link Dao}, хранящая каждый ключ в отдельном файле на диске.
 */
public class FileByteDao implements Dao<byte[]> {

    private final Path directory;

    /**
     * Создать хранилище в указанной директории.
     *
     * @param directory директория данных, создается при необходимости
     * @throws IOException если директорию не удалось создать
     */
    public FileByteDao(Path directory) throws IOException {
        this.directory = directory.toAbsolutePath();
        Files.createDirectories(this.directory);
    }

    @Override
    public byte[] get(String key) throws NoSuchElementException {
        try {
            return Files.readAllBytes(pathOf(key));
        } catch (NoSuchFileException exception) {
            throw new NoSuchElementException("no value stored for key: " + key, exception);
        } catch (IOException exception) {
            throw new UncheckedIOException(exception);
        }
    }

    @Override
    public void upsert(String key, byte[] value) throws IOException {
        var target = pathOf(key);
        var temp = Files.createTempFile(directory, ".kv-", ".tmp");
        try {
            Files.write(temp, value);
            Files.move(temp, target, StandardCopyOption.REPLACE_EXISTING);
        } finally {
            Files.deleteIfExists(temp);
        }
    }

    @Override
    public void delete(String key) throws IOException {
        Files.deleteIfExists(pathOf(key));
    }

    @Override
    public void close() {
        // Состояние всегда записывается на диск
    }

    private Path pathOf(String key) {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("key must be non-empty");
        }
        var encoded = Base64.getUrlEncoder().withoutPadding()
            .encodeToString(key.getBytes(StandardCharsets.UTF_8));
        return directory.resolve(encoded);
    }
}
