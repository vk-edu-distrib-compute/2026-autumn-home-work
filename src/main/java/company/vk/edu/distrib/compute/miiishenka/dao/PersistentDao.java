package company.vk.edu.distrib.compute.miiishenka.dao;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Base64;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

import company.vk.edu.distrib.compute.Dao;

public class PersistentDao<T> implements Dao<T> {
    private static final byte UPSERT = 1;
    private static final byte DELETE = 2;
    private static final String EMPTY_VALUE = "";

    private final Function<T, String> serialize;
    private final Function<String, T> deserialize;
    private final RandomAccessFile journal;
    private final Map<String, T> storage = new ConcurrentHashMap<>();
    private final Lock persistenceLock = new ReentrantLock();

    public PersistentDao(
            String fileName,
            Function<T, String> serialize,
            Function<String, T> deserialize
    ) throws IOException {
        this.serialize = serialize;
        this.deserialize = deserialize;
        journal = new RandomAccessFile(fileName, "rw");
        try {
            replayJournal();
        } catch (IOException | RuntimeException e) {
            try {
                journal.close();
            } catch (IOException closeException) {
                e.addSuppressed(closeException);
            }
            throw e;
        }
    }

    public static PersistentDao<String> stringDao(String fileName) throws IOException {
        return new PersistentDao<>(
                fileName,
                Function.identity(),
                Function.identity()
        );
    }

    public static PersistentDao<byte[]> byteArrayDao(String fileName) throws IOException {
        return new PersistentDao<>(
                fileName,
                Base64.getEncoder()::encodeToString,
                Base64.getDecoder()::decode
        );
    }

    @Override
    public T get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        T value = storage.get(key);
        if (value == null) {
            throw new NoSuchElementException("No value for key: " + key);
        }
        return value;
    }

    @Override
    public void upsert(String key, T value) throws IllegalArgumentException, IOException {
        String serializedValue = serialize.apply(value);
        byte[] record = encodeRecord(UPSERT, key, serializedValue);
        persistenceLock.lock();
        try {
            append(record);
            storage.put(key, value);
        } finally {
            persistenceLock.unlock();
        }
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        byte[] record = encodeRecord(DELETE, key, EMPTY_VALUE);
        persistenceLock.lock();
        try {
            append(record);
            storage.remove(key);
        } finally {
            persistenceLock.unlock();
        }
    }

    @Override
    public void close() throws IOException {
        persistenceLock.lock();
        try {
            journal.close();
        } finally {
            persistenceLock.unlock();
        }
    }

    private void append(byte[] record) throws IOException {
        long recordStart = journal.getFilePointer();
        try {
            journal.write(record);
        } catch (IOException e) {
            try {
                journal.setLength(recordStart);
            } catch (IOException rollbackException) {
                e.addSuppressed(rollbackException);
            }
            throw e;
        }
    }

    private byte[] encodeRecord(byte operation, String key, String value) throws IOException {
        try (ByteArrayOutputStream buffer = new ByteArrayOutputStream();
                DataOutputStream output = new DataOutputStream(buffer)) {
            output.writeByte(operation);
            output.writeUTF(key);
            output.writeUTF(value);
            return buffer.toByteArray();
        }
    }

    private void replayJournal() throws IOException {
        journal.seek(0);

        while (journal.getFilePointer() < journal.length()) {
            long recordStart = journal.getFilePointer();

            try {
                byte operation = journal.readByte();
                String key = journal.readUTF();
                String value = journal.readUTF();

                switch (operation) {
                    case UPSERT -> storage.put(key, deserialize.apply(value));
                    case DELETE -> storage.remove(key);
                    default -> throw new IOException("Unknown journal operation: " + operation);
                }
            } catch (EOFException e) {
                journal.setLength(recordStart);
                break;
            }
        }
    }
}
