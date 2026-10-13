package company.vk.edu.distrib.compute.annapiatkova.kv;

import company.vk.edu.distrib.compute.Dao;

import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.Files;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.concurrent.locks.ReentrantLock;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.NoSuchElementException;

class ByteArrayDao implements Dao<byte[]> {
    private final DataOutputStream ostr;
    private final ReentrantLock lock = new ReentrantLock();
    private final Map<String, byte[]> values = new ConcurrentHashMap<>();
    private static final byte PUT = 1;
    private static final byte DELETE = 2;

    ByteArrayDao(Path path) throws IOException {
        if (!Files.exists(path)) {
            Files.createFile(path);
        }
        try (DataInputStream istr = new DataInputStream(Files.newInputStream(path))) {
            readDataFromFile(istr);
            istr.close();
        }
        ostr = new DataOutputStream(Files.newOutputStream(path, StandardOpenOption.APPEND));
    }

    private void readDataFromFile(DataInputStream istr) throws IOException {
        byte[] op = new byte[1];
        while (istr.read(op) != -1) {
            if (op[0] == PUT) {
                String key = istr.readUTF();
                int length = istr.readInt();
                values.put(key, readBytes(istr, length));
            } else if (op[0] == DELETE) {
                String key = istr.readUTF();
                values.remove(key);
            } else {
                throw new IOException("Malformed log file");
            }
        }
    }

    private byte[] readBytes(DataInputStream istr, int n) throws IOException {
        byte[] value = new byte[n];
        istr.read(value, 0, n);
        return value;
    }

    @Override
    public byte[] get(String key) {
        byte[] value = values.get(key);
        if (value == null) {
            throw new NoSuchElementException();
        }
        return value;
    }

    @Override
    public void upsert(String key, byte[] value) throws IOException {
        lock.lock();
        try {
            values.put(key, value);
            ostr.writeByte(PUT);
            ostr.writeUTF(key);
            ostr.writeInt(value.length);
            ostr.write(value, 0, value.length);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void delete(String key) throws IOException {
        lock.lock();
        try {
            values.remove(key);
            ostr.writeByte(DELETE);
            ostr.writeUTF(key);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() {
        //
    }

}
