package company.vk.edu.distrib.compute.iizhukov.urlshortener.db;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.RecordComponent;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class FileStorage<T extends Record> implements Closeable {
    private final RandomAccessFile file;
    private final Class<T> type;

    public FileStorage(File file, Class<T> type) throws IOException {
        File parent = file.getParentFile();

        if (parent != null) {
            parent.mkdirs();
        }

        this.file = new RandomAccessFile(file, "rw");
        this.type = type;
    }

    public void write(Map<String, T> data) throws IOException {
        file.setLength(0);
        file.writeInt(data.size());

        for (var entry : data.entrySet()) {
            String key = entry.getKey();
            T value = entry.getValue();

            file.writeUTF(key);

            for (var item : toArray(value)) {
                file.writeUTF(item);
            }
        }
    }

    public Map<String, T> read() throws IOException {
        var result = new HashMap<String, T>();
        file.seek(0);

        if (file.length() == 0) {
            return result;
        }

        var count = file.readInt();

        for (int i = 0; i < count; ++i) {
            var key = file.readUTF();

            result.put(
                    key,
                    readRecord()
            );
        }

        return result;
    }

    private T readRecord() throws IOException {
        var count = type.getRecordComponents().length;
        var values = new String[count];

        for (int i = 0; i < count; ++i) {
            values[i] = file.readUTF();
        }

        try {
            return getConstructor().newInstance((Object[]) values);
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    private Constructor<T> getConstructor() {
        var components = type.getRecordComponents();

        Class<?>[] parameterTypes = Arrays.stream(components)
                .map(RecordComponent::getType)
                .toArray(Class<?>[]::new);

        try {
            return type.getDeclaredConstructor(parameterTypes);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException("No such constructor in record", e);
        }
    }

    public static String[] toArray(Record record) {
        var components = record.getClass().getRecordComponents();

        String[] result = new String[components.length];

        for (int i = 0; i < components.length; i++) {
            try {
                result[i] = (String) components[i]
                        .getAccessor()
                        .invoke(record);
            } catch (ReflectiveOperationException e) {
                throw new RuntimeException(e);
            }
        }

        return result;
    }

    @Override
    public void close() throws IOException {
        file.close();
    }
}
