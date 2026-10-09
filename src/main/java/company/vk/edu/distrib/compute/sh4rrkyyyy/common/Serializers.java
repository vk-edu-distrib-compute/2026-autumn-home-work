package company.vk.edu.distrib.compute.sh4rrkyyyy.common;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public final class Serializers {
    private Serializers() {
    }

    public static final Serializer<String> STRING = new Serializer<>() {
        @Override
        public void write(DataOutputStream out, String value) throws IOException {
            out.writeUTF(value);
        }

        @Override
        public String read(DataInputStream in) throws IOException {
            return in.readUTF();
        }
    };

    public static final Serializer<byte[]> BYTES = new Serializer<>() {
        @Override
        public void write(DataOutputStream out, byte[] value) throws IOException {
            out.writeInt(value.length);
            out.write(value);
        }

        @Override
        public byte[] read(DataInputStream in) throws IOException {
            byte[] data = new byte[in.readInt()];
            in.readFully(data);
            return data;
        }
    };
}
