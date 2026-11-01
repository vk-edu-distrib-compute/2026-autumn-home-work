package company.vk.edu.distrib.compute.fedorkhokhryakov.kv;

import java.nio.charset.StandardCharsets;
import java.util.Comparator;
import java.util.List;
import java.util.stream.IntStream;

public final class RendezvousHashing {
    private static final long FNV_OFFSET_BASIS = 0xcbf29ce484222325L;
    private static final long FNV_PRIME = 0x100000001b3L;
    private static final long MIX_MULTIPLIER_1 = 0xbf58476d1ce4e5b9L;
    private static final long MIX_MULTIPLIER_2 = 0x94d049bb133111ebL;

    private final long[] nodeSeeds;

    public RendezvousHashing(List<String> nodes) {
        if (nodes.isEmpty()) {
            throw new IllegalArgumentException("At least one node is required");
        }

        nodeSeeds = new long[nodes.size()];
        for (int i = 0; i < nodeSeeds.length; i++) {
            long seed = fnv1a(FNV_OFFSET_BASIS, nodes.get(i).getBytes(StandardCharsets.UTF_8));
            nodeSeeds[i] = seed * FNV_PRIME;
        }
    }

    public int nodeIndex(String key) {
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        int bestNode = 0;
        long bestScore = score(nodeSeeds[0], keyBytes);
        for (int i = 1; i < nodeSeeds.length; i++) {
            long nodeScore = score(nodeSeeds[i], keyBytes);
            if (nodeScore > bestScore) {
                bestNode = i;
                bestScore = nodeScore;
            }
        }
        return bestNode;
    }

    public int[] topNodes(String key, int count) {
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        long[] scores = new long[nodeSeeds.length];
        for (int i = 0; i < nodeSeeds.length; i++) {
            scores[i] = score(nodeSeeds[i], keyBytes);
        }
        return IntStream.range(0, nodeSeeds.length)
            .boxed()
            .sorted(Comparator.comparingLong((Integer node) -> scores[node]).reversed())
            .limit(count)
            .mapToInt(Integer::intValue)
            .toArray();
    }

    private static long score(long nodeSeed, byte[] keyBytes) {
        return mix(fnv1a(nodeSeed, keyBytes));
    }

    private static long fnv1a(long seed, byte[] bytes) {
        long hash = seed;
        for (byte value : bytes) {
            hash = (hash ^ (value & 0xff)) * FNV_PRIME;
        }
        return hash;
    }

    private static long mix(long hash) {
        long mixed = (hash ^ (hash >>> 30)) * MIX_MULTIPLIER_1;
        mixed = (mixed ^ (mixed >>> 27)) * MIX_MULTIPLIER_2;
        return mixed ^ (mixed >>> 31);
    }
}
