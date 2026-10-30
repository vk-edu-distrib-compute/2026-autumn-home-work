package company.vk.edu.distrib.compute.fedorkhokhryakov.kv;

import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ClusterRemoteDao implements Dao<String> {
    private final List<KVRemoteDao> nodes;
    private final RendezvousHashing hashing;

    public ClusterRemoteDao(int... ports) {
        if (ports.length == 0) {
            throw new IllegalArgumentException("At least one KV service port is required");
        }
        if (Arrays.stream(ports).distinct().count() != ports.length) {
            throw new IllegalArgumentException("KV service ports must be unique: " + Arrays.toString(ports));
        }

        List<String> names = new ArrayList<>(ports.length);
        List<KVRemoteDao> remoteDaos = new ArrayList<>(ports.length);
        for (int port : ports) {
            names.add("localhost:" + port);
            remoteDaos.add(new KVRemoteDao(port));
        }
        this.nodes = List.copyOf(remoteDaos);
        this.hashing = new RendezvousHashing(names);
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
        nodes.forEach(KVRemoteDao::close);
    }

    private KVRemoteDao nodeFor(String key) {
        return nodes.get(hashing.nodeIndex(key));
    }
}
