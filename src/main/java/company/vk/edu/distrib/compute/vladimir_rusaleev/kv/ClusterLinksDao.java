package company.vk.edu.distrib.compute.vladimir_rusaleev.kv;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import company.vk.edu.distrib.compute.Dao;

final class ClusterLinksDao implements Dao<String> {
    private final int[] ports;
    private final RemoteLinksDao[] nodes;

    ClusterLinksDao(int... ports) {
        this.ports = ports.clone();
        nodes = new RemoteLinksDao[ports.length];

        for (int i = 0; i < ports.length; i++) {
            nodes[i] = new RemoteLinksDao(ports[i]);
        }
    }

    @Override
    public String get(String key) throws IOException {
        return nodeFor(key).get(key);
    }

    @Override
    public void upsert(String key, String value) throws IOException {
        nodeFor(key).upsert(key, value);
    }

    @Override
    public void delete(String key) throws IOException {
        nodeFor(key).delete(key);
    }

    @Override
    public void close() {
        for (RemoteLinksDao node : nodes) {
            node.close();
        }
    }

    private RemoteLinksDao nodeFor(String key) {
        return nodes[ownerIndex(key, ports)];
    }

    static int ownerIndex(String key, int... ports) {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("Empty key");
        }

        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        MessageDigest digest = newDigest();
        int selected = 0;
        long best = 0;

        for (int i = 0; i < ports.length; i++) {
            digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(ports[i]).array());
            long score = ByteBuffer.wrap(digest.digest(keyBytes)).getLong();

            if (i == 0 || Long.compareUnsigned(score, best) > 0) {
                selected = i;
                best = score;
            }
        }

        return selected;
    }

    private static MessageDigest newDigest() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException("Unavailable", exception);
        }
    }
}
