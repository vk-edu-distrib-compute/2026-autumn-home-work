package company.vk.edu.distrib.compute.virogg.kv;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.OverlappingFileLockException;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.CRC32;

import company.vk.edu.distrib.compute.Dao;

public final class PersistentByteDao implements Dao<byte[]> {
    private static final int HEADER_SIZE = Integer.BYTES * 3 + Byte.BYTES;
    private static final int RECORD_OVERHEAD = HEADER_SIZE + Integer.BYTES;
    private static final byte UPSERT = 0;
    private static final byte DELETE = 1;

    private final Path directory;
    private final ReentrantLock operations = new ReentrantLock();
    private final ConcurrentMap<String, byte[]> data = new ConcurrentHashMap<>();
    private final FileChannel lockFile;
    private final FileChannel journal;
    private boolean closed;
    private boolean failed;
    private boolean writeFailed;

    public PersistentByteDao(Path directory) throws IOException {
        this.directory = Files.createDirectories(directory);
        lockFile = FileChannel.open(directory.resolve("kv.lock"),
                StandardOpenOption.CREATE, StandardOpenOption.WRITE);
        try {
            acquireLock(lockFile);
            journal = FileChannel.open(directory.resolve("kv.log"),
                    StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
            try {
                recover();
            } catch (IOException | RuntimeException e) {
                closeSuppressed(journal, e);
                throw e;
            }
        } catch (IOException | RuntimeException e) {
            closeSuppressed(lockFile, e);
            throw e;
        }
    }

    @Override
    public byte[] get(String key) throws IOException {
        operations.lock();
        try {
            requireKey(key);
            ensureOpen();
            byte[] value = data.get(key);
            if (value == null) {
                throw new NoSuchElementException("No value for key " + key);
            }
            return value.clone();
        } finally {
            operations.unlock();
        }
    }

    @Override
    public void upsert(String key, byte[] value) throws IOException {
        operations.lock();
        try {
            requireKey(key);
            ensureOpen();
            byte[] copy = Objects.requireNonNull(value, "value").clone();
            append(key, copy, UPSERT);
            data.put(key, copy);
        } finally {
            operations.unlock();
        }
    }

    @Override
    public void delete(String key) throws IOException {
        operations.lock();
        try {
            requireKey(key);
            ensureOpen();
            append(key, new byte[0], DELETE);
            data.remove(key);
        } finally {
            operations.unlock();
        }
    }

    public boolean isWritable() {
        operations.lock();
        try {
            return !closed && !failed && !writeFailed && journal.isOpen() && lockFile.isOpen()
                    && Files.isWritable(directory) && Files.isWritable(directory.resolve("kv.log"));
        } finally {
            operations.unlock();
        }
    }

    @Override
    public void close() throws IOException {
        operations.lock();
        try {
            if (closed) {
                return;
            }
            closed = true;
            try (lockFile) {
                journal.close();
            }
        } finally {
            operations.unlock();
        }
    }

    private void ensureOpen() throws IOException {
        if (closed || failed || !journal.isOpen()) {
            throw new IOException("Storage is closed or unavailable");
        }
    }

    private void append(String key, byte[] value, byte operation) throws IOException {
        ByteBuffer record = RecordCodec.encode(key, value, operation);
        long start;
        try {
            start = journal.size();
        } catch (IOException e) {
            writeFailed = true;
            throw e;
        }
        try {
            journal.position(start);
            while (record.hasRemaining()) {
                journal.write(record);
            }
            journal.force(true);
            writeFailed = false;
        } catch (IOException e) {
            writeFailed = true;
            try {
                journal.truncate(start);
                journal.position(start);
                journal.force(true);
            } catch (IOException rollbackFailure) {
                failed = true;
                e.addSuppressed(rollbackFailure);
            }
            throw e;
        }
    }

    private void recover() throws IOException {
        long size = journal.size();
        long position = 0;
        ByteBuffer header = ByteBuffer.allocate(HEADER_SIZE);
        while (position < size) {
            if (size - position < HEADER_SIZE) {
                truncateTail(position);
                return;
            }
            header.clear();
            readFully(header, position);
            header.flip();
            int length = header.getInt();
            int keyLength = header.getInt();
            int valueLength = header.getInt();
            byte operation = header.get();
            RecordCodec.validateHeader(length, keyLength, valueLength, operation);
            if (length > size - position) {
                truncateTail(position);
                return;
            }
            readAndApplyRecord(header, position, length, keyLength, valueLength, operation);
            position += length;
        }
        journal.position(position);
    }

    private void readAndApplyRecord(ByteBuffer header, long position, int length, int keyLength,
                                   int valueLength, byte operation) throws IOException {
        ByteBuffer record = ByteBuffer.allocate(length);
        record.put(header.array());
        readFully(record, position + HEADER_SIZE);
        RecordCodec.verifyChecksum(record);
        record.flip().position(HEADER_SIZE);
        byte[] keyBytes = new byte[keyLength];
        record.get(keyBytes);
        String key = RecordCodec.decodeKey(keyBytes);
        if (operation == DELETE) {
            data.remove(key);
        } else {
            byte[] value = new byte[valueLength];
            record.get(value);
            data.put(key, value);
        }
    }

    private void readFully(ByteBuffer buffer, long position) throws IOException {
        long offset = position;
        while (buffer.hasRemaining()) {
            int count = journal.read(buffer, offset);
            if (count < 0) {
                throw new EOFException("Journal changed during recovery");
            }
            offset += count;
        }
    }

    private void truncateTail(long position) throws IOException {
        journal.truncate(position);
        journal.position(position);
        journal.force(true);
    }

    private static void requireKey(String key) {
        Objects.requireNonNull(key, "key");
        if (key.isEmpty()) {
            throw new IllegalArgumentException("Key must not be empty");
        }
    }

    private static void acquireLock(FileChannel channel) throws IOException {
        try {
            if (channel.tryLock() != null) {
                return;
            }
        } catch (OverlappingFileLockException expected) {
        }
        throw new IOException("Storage directory is already in use");
    }

    private static void closeSuppressed(FileChannel channel, Exception failure) {
        try {
            channel.close();
        } catch (IOException closeFailure) {
            failure.addSuppressed(closeFailure);
        }
    }

    private static final class RecordCodec {
        private static ByteBuffer encode(String key, byte[] value, byte operation) throws IOException {
            byte[] keyBytes = encodeKey(key);
            long length = (long) RECORD_OVERHEAD + keyBytes.length + value.length;
            if (length > Integer.MAX_VALUE) {
                throw new IllegalArgumentException("Record is too large");
            }
            ByteBuffer record = ByteBuffer.allocate((int) length);
            record.putInt((int) length).putInt(keyBytes.length).putInt(value.length).put(operation);
            record.put(keyBytes).put(value);
            CRC32 checksum = new CRC32();
            checksum.update(record.array(), 0, record.position());
            record.putInt((int) checksum.getValue()).flip();
            return record;
        }

        private static void validateHeader(int length, int keyLength, int valueLength, byte operation)
                throws IOException {
            long expectedLength = (long) RECORD_OVERHEAD + keyLength + valueLength;
            if (keyLength <= 0 || valueLength < 0 || length != expectedLength
                    || !validOperation(operation, valueLength)) {
                throw new IOException("Invalid journal record header");
            }
        }

        private static boolean validOperation(byte operation, int valueLength) {
            return operation == UPSERT || operation == DELETE && valueLength == 0;
        }

        private static void verifyChecksum(ByteBuffer record) throws IOException {
            int payloadLength = record.capacity() - Integer.BYTES;
            CRC32 checksum = new CRC32();
            checksum.update(record.array(), 0, payloadLength);
            if ((int) checksum.getValue() != record.getInt(payloadLength)) {
                throw new IOException("Journal record checksum mismatch");
            }
        }

        private static byte[] encodeKey(String key) throws IOException {
            ByteBuffer encoded = StandardCharsets.UTF_8.newEncoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .encode(java.nio.CharBuffer.wrap(key));
            byte[] bytes = new byte[encoded.remaining()];
            encoded.get(bytes);
            return bytes;
        }

        private static String decodeKey(byte[] bytes) throws CharacterCodingException {
            return StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(bytes)).toString();
        }
    }
}
