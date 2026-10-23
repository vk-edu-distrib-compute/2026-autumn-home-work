package company.vk.edu.distrib.compute.playingpeano.kv;

import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.util.NoSuchElementException;

final class ClusterStringDao implements Dao<String> {
    private static final long FNV_OFFSET = 0xcbf29ce484222325L;
    private static final long FNV_PRIME = 0x100000001b3L;
    private static final int NOT_HIGHER = 0;

    private final int[] ports;
    private final RemoteStringDao[] nodes;

    ClusterStringDao(int... ports) {
        this.ports = ports.clone();
        nodes = new RemoteStringDao[ports.length];
        for (int index = 0; index < ports.length; index++) {
            nodes[index] = new RemoteStringDao(ports[index]);
        }
    }

    @Override
    public String get(String key) throws NoSuchElementException, IOException {
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
        for (RemoteStringDao node : nodes) {
            node.close();
        }
    }

    private RemoteStringDao nodeFor(String key) {
        int owner = 0;
        long ownerScore = rendezvousScore(key, ports[0]);
        for (int index = 1; index < ports.length; index++) {
            long candidateScore = rendezvousScore(key, ports[index]);
            if (outranks(candidateScore, ownerScore)) {
                owner = index;
                ownerScore = candidateScore;
            }
        }
        return nodes[owner];
    }

    private static boolean outranks(long candidateScore, long ownerScore) {
        return Long.compareUnsigned(candidateScore, ownerScore) > NOT_HIGHER;
    }

    private static long rendezvousScore(String key, int port) {
        long hash = FNV_OFFSET;
        for (int index = 0; index < key.length(); index++) {
            hash ^= key.charAt(index);
            hash *= FNV_PRIME;
        }
        hash ^= port & 0xffffffffL;
        hash *= FNV_PRIME;
        return hash;
    }
}
