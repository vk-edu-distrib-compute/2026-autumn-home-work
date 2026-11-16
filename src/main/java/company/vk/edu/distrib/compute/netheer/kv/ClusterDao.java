package company.vk.edu.distrib.compute.netheer.kv;

import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;

public class ClusterDao implements Dao<String> {
    private final Map<Integer, RemoteDao> nodes;

    public ClusterDao(int... ports) {
        this.nodes = new HashMap<>();

        for (int port : ports) {
            nodes.computeIfAbsent(port, RemoteDao::new);
        }
    }

    @Override
    public void close() {
        for (RemoteDao node : nodes.values()) {
            node.close();
        }
    }

    private RemoteDao selectNode(String key) {
        long bestScore = 0;
        int choosedPort = -1;
        RemoteDao choosedDao = null;

        for (Map.Entry<Integer, RemoteDao> entry : nodes.entrySet()) {
            int port = entry.getKey();
            RemoteDao dao = entry.getValue();

            long score = score(key, port);

            if (choosedDao == null
                    || Long.compareUnsigned(score, bestScore) > 0
                    || (score == bestScore && port < choosedPort)) {
                choosedDao = dao;
                choosedPort = port;
                bestScore = score;
            }
        }

        if (choosedDao == null) {
            throw new IllegalArgumentException("Cluster is empty");
        }

        return choosedDao;
    }

    private static long score(String key, int port) {
        byte[] input = (key + ":" + port).getBytes(StandardCharsets.UTF_8);
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(input);
            return ByteBuffer.wrap(hash).getLong();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 using failed", e);
        }
    }

    @Override
    public String get(String key) throws IOException {
        RemoteDao node = this.selectNode(key);
        return node.get(key);
    }

    @Override
    public void upsert(String key, String value) throws IOException {
        RemoteDao node = this.selectNode(key);
        node.upsert(key, value);
    }

    @Override
    public void delete(String key) throws IOException {
        RemoteDao node = this.selectNode(key);
        node.delete(key);
    }
}
