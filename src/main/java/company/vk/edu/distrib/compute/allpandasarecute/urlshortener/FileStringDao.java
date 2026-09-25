package company.vk.edu.distrib.compute.allpandasarecute.urlshortener;

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.NoSuchElementException;

import company.vk.edu.distrib.compute.Dao;

public class FileStringDao implements Dao<String> {
    private final Path directory;

    public FileStringDao(Path directory) throws IOException {
        Files.createDirectories(directory);
        this.directory = directory;
    }

    @Override
    public String get(String key) throws NoSuchElementException, IOException {
        Path file = fileFor(key);
        if (!Files.exists(file)) {
            throw new NoSuchElementException("No value for key '%s'".formatted(key));
        }
        return Files.readString(file, StandardCharsets.UTF_8);
    }

    @Override
    public void upsert(String key, String value) throws IOException {
        Files.writeString(fileFor(key), value, StandardCharsets.UTF_8);
    }

    @Override
    public void delete(String key) throws IOException {
        Files.deleteIfExists(fileFor(key));
    }

    @Override
    public void close() {
        //
    }

    private Path fileFor(String key) {
        if (key.isBlank()) {
            throw new IllegalArgumentException("Key must not be blank");
        }
        return directory.resolve(URLEncoder.encode(key, StandardCharsets.UTF_8));
    }
}
