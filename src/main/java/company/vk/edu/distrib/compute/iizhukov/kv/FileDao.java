package company.vk.edu.distrib.compute.iizhukov.kv;

import java.io.EOFException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

import company.vk.edu.distrib.compute.Dao;

final class FileDao implements Dao<byte[]> {
    private static final byte PUT = 1;
    private static final byte DELETE = 2;

    private final Map<String, byte[]> data = new ConcurrentHashMap<>();
    private final RandomAccessFile file;
    private final ReentrantLock lock = new ReentrantLock();

    FileDao(Path path) throws IOException {
        Files.createDirectories(path.toAbsolutePath().getParent());
        file = new RandomAccessFile(path.toFile(), "rw");
        load();
        compact();
    }

    @Override
    public byte[] get(String key) {
        var value = data.get(key);

        if (value == null) {
            throw new NoSuchElementException();
        }

        return value;
    }

    @Override
    public void upsert(String key, byte[] value) throws IOException {
        lock.lock();
        try {
            append(PUT, key, value);
            data.put(key, value);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void delete(String key) throws IOException {
        lock.lock();
        try {
            append(DELETE, key, new byte[0]);
            data.remove(key);
        } finally {
            lock.unlock();
        }
    }

    boolean isOpen() {
        return file.getChannel().isOpen();
    }

    private void load() throws IOException {
        file.seek(0);

        while (file.getFilePointer() < file.length()) {
            long position = file.getFilePointer();

            try {
                byte command = file.readByte();
                String key = file.readUTF();

                if (command == PUT) {
                    data.put(key, readValue());
                } else if (command == DELETE) {
                    data.remove(key);
                } else {
                    throw new IOException("Unknown command: " + command);
                }
            } catch (EOFException e) {
                file.setLength(position);
                break;
            }
        }
    }

    private byte[] readValue() throws IOException {
        var value = new byte[file.readInt()];
        file.readFully(value);
        return value;
    }

    private void compact() throws IOException {
        file.setLength(0);
        file.seek(0);

        for (var entry : data.entrySet()) {
            append(PUT, entry.getKey(), entry.getValue());
        }
    }

    private void append(byte command, String key, byte[] value) throws IOException {
        long position = file.length();
        file.seek(position);

        try {
            file.writeByte(command);
            file.writeUTF(key);

            if (command == PUT) {
                file.writeInt(value.length);
                file.write(value);
            }
        } catch (IOException e) {
            file.setLength(position);
            throw e;
        }
    }

    @Override
    public void close() throws IOException {
        lock.lock();
        try {
            file.close();
        } finally {
            lock.unlock();
        }
    }
}
