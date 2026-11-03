package company.vk.edu.distrib.compute.cloudyy74.kv;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

final class Cloudyy74HashUtils {
    private Cloudyy74HashUtils() {
    }

    static long hash(String key, int port) {
        try {
            final var digest = MessageDigest.getInstance("SHA-256");
            final var value = key + ":" + port;
            final var bytes = digest.digest(value.getBytes(StandardCharsets.UTF_8));
            return ByteBuffer.wrap(bytes).getLong();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is not available", e);
        }
    }
}
