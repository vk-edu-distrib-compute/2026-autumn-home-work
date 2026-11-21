package company.vk.edu.distrib.compute.iizhukov.kv;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.kv.ClusterDaoFactoryTest;
import company.vk.edu.distrib.compute.kv.RemoteDaoFactory;

@ClusterDaoFactoryTest
public final class ClusterRemoteDaoFactory implements RemoteDaoFactory<String> {
    private static final int MIN_PORT = 1;
    private static final int MAX_PORT = 65535;

    @Override
    public Dao<String> create(int... ports) {
        if (ports.length == 0) {
            throw new IllegalArgumentException("Port is required");
        }

        int[] nodes = Arrays.stream(ports).distinct().sorted().toArray();
        for (int port : nodes) {
            validatePort(port);
        }

        return new HttpDao(key -> nodeForKey(key, nodes));
    }

    private static void validatePort(int port) {
        if (port < MIN_PORT || MAX_PORT < port) {
            throw new IllegalArgumentException("invalide port: " + port);
        }
    }

    private static int nodeForKey(String key, int... nodes) {
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        var digest = newDigest();
        int selected = nodes[0];
        long bestScore = 0;

        for (int port : nodes) {
            digest.update(keyBytes);
            digest.update((byte) 0);
            byte[] hash = digest.digest(Integer.toString(port).getBytes(StandardCharsets.UTF_8));
            long score = ByteBuffer.wrap(hash).getLong();

            if (Long.compareUnsigned(score, bestScore) > 0) {
                selected = port;
                bestScore = score;
            }
        }

        return selected;
    }

    private static MessageDigest newDigest() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
    }
}
