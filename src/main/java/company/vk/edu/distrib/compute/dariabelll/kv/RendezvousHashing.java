package company.vk.edu.distrib.compute.dariabelll.kv;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

public class RendezvousHashing {

    private final int[] ports;

    public RendezvousHashing(int... ports) {
        this.ports = ports.clone();
    }

    public int[] selectPortIndices(String key, int count) {
        if (count < 1 || count > ports.length) {
            throw new IllegalArgumentException("Invalid replica count");
        }

        record PortHash(int index, long hash) {}

        PortHash[] portHashes = new PortHash[this.ports.length];

        for (int i = 0; i < this.ports.length; ++i) {
            portHashes[i] = new PortHash(i, computeHash(key, ports[i]));
        }

        Arrays.sort(portHashes, (a, b) -> {
            int cmp = Long.compare(b.hash(), a.hash());
            if (cmp == 0) {
                return Integer.compare(ports[a.index()], ports[b.index()]);
            }
            return cmp;
        });

        return Arrays.stream(portHashes)
                .limit(count)
                .mapToInt(PortHash::index)
                .toArray();
    }

    private static long computeHash(String key, int port) {
        String input = key + "|" + port;
        byte[] bytes = input.getBytes(StandardCharsets.UTF_8);

        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hashBytes = digest.digest(bytes);

            return ByteBuffer.wrap(hashBytes).getLong();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is not available", e);
        }
    }
}
