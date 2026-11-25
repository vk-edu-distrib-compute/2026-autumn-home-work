package company.vk.edu.distrib.compute.akhunzianov;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.NoSuchElementException;
import java.util.concurrent.locks.ReentrantLock;

import company.vk.edu.distrib.compute.Dao;

public class PersistentDao implements Dao<String> {

    private static final String UPSERT = "+";
    private static final String DELETE = "-";
    private static final String SEPARATOR = "\t";
    private static final int FIELDS = 3;

    private final ReentrantLock lock = new ReentrantLock();

    private final InMemoryDao mem = new InMemoryDao();
    private final BufferedWriter log;

    public PersistentDao(Path path) throws IOException {
        load(path);
        log = Files.newBufferedWriter(path, StandardCharsets.UTF_8,
            StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }

    @Override
    public String get(String key) throws NoSuchElementException {
        lock.lock();
        try {
            return mem.get(key);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void upsert(String key, String value) throws IOException {
        lock.lock();
        try {
            append(UPSERT, key, value);
            mem.upsert(key, value);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void delete(String key) throws IOException {
        lock.lock();
        try {
            append(DELETE, key, "");
            mem.delete(key);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() throws IOException {
        lock.lock();
        try {
            log.close();
            mem.close();
        } finally {
            lock.unlock();
        }
    }

    private void load(Path path) throws IOException {
        if (!Files.exists(path)) {
            return;
        }
        for (var line : Files.readAllLines(path, StandardCharsets.UTF_8)) {
            var record = line.split(SEPARATOR, FIELDS);
            if (record.length < FIELDS) {
                continue;
            }
            if (UPSERT.equals(record[0])) {
                mem.upsert(record[1], record[2]);
            } else {
                mem.delete(record[1]);
            }
        }
    }

    private void append(String op, String key, String value) throws IOException {
        log.write(op + SEPARATOR + key + SEPARATOR + value);
        log.newLine();
        log.flush();
    }
}
