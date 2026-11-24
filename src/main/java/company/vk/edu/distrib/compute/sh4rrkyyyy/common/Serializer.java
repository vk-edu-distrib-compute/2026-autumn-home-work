package company.vk.edu.distrib.compute.sh4rrkyyyy.common;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public interface Serializer<T> {
    void write(DataOutputStream out, T value) throws IOException;

    T read(DataInputStream in) throws IOException;
}
