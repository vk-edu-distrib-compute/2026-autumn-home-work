package company.vk.edu.distrib.compute.akhunzianov.kv;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;

import company.vk.edu.distrib.compute.Dao;

public class ClusterDao implements Dao<String> {

    private static final long FNV_OFFSET = 0xcbf29ce484222325L;
    private static final long FNV_PRIME = 0x100000001b3L;
    private static final long MIX_1 = 0xbf58476d1ce4e5b9L;
    private static final long MIX_2 = 0x94d049bb133111ebL;
    private static final int BYTE_MASK = 0xff;

    private final List<RemoteDao> nodes;
    private final byte[][] nodeIds;

    public ClusterDao(int... ports) {
        if (ports.length == 0) {
            throw new IllegalArgumentException("Cluster needs at least one node");
        }
        this.nodes = Arrays.stream(ports).mapToObj(RemoteDao::new).toList();
        this.nodeIds = new byte[ports.length][];
        for (int i = 0; i < ports.length; i++) {
            nodeIds[i] = ("#" + i).getBytes(StandardCharsets.UTF_8);
        }
    }

    @Override
    public String get(String key) throws NoSuchElementException, IOException {
        return ownerOf(key).get(key);
    }

    @Override
    public void upsert(String key, String value) throws IOException {
        ownerOf(key).upsert(key, value);
    }

    @Override
    public void delete(String key) throws IOException {
        ownerOf(key).delete(key);
    }

    @Override
    public void close() {
        nodes.forEach(RemoteDao::close);
    }

    private RemoteDao ownerOf(String key) {
        var keyHash = feed(FNV_OFFSET, key.getBytes(StandardCharsets.UTF_8));
        var best = 0;
        var bestScore = Long.MIN_VALUE;
        for (int i = 0; i < nodeIds.length; i++) {
            var score = mix(feed(keyHash, nodeIds[i]));
            if (score > bestScore) {
                bestScore = score;
                best = i;
            }
        }
        return nodes.get(best);
    }

    private static long feed(long hash, byte[] bytes) {
        var result = hash;
        for (byte b : bytes) {
            result ^= b & BYTE_MASK;
            result *= FNV_PRIME;
        }
        return result;
    }

    private static long mix(long value) {
        var z = (value ^ (value >>> 30)) * MIX_1;
        z = (z ^ (z >>> 27)) * MIX_2;
        return z ^ (z >>> 31);
    }
}
