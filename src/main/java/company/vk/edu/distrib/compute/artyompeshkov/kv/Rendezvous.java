package company.vk.edu.distrib.compute.artyompeshkov.kv;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Comparator;
import java.util.List;

final class Rendezvous {
    private Rendezvous() {
    }

    static List<Integer> rank(String key, List<Integer> ports) {
        return ports.stream()
                .sorted(Comparator.comparingLong((Integer port) -> hash(port + ":" + key)).reversed())
                .toList();
    }

    private static long hash(String value) {
        try {
            byte[] result = MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8));
            return ByteBuffer.wrap(result).getLong();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is not supported", e);
        }
    }
}
