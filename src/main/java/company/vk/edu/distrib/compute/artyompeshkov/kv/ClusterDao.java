package company.vk.edu.distrib.compute.artyompeshkov.kv;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import company.vk.edu.distrib.compute.Dao;

class ClusterDao implements Dao<String> {
    private final List<Integer> ports;
    private final List<RemoteDao> nodes;

    ClusterDao(int... ports) {
        if (ports.length == 0) {
            throw new IllegalArgumentException("Cluster must contain at least one node");
        }
        this.ports = Arrays.stream(ports).boxed().toList();
        this.nodes = Arrays.stream(ports).mapToObj(RemoteDao::new).toList();
    }

    @Override
    public String get(String key) throws IOException {
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
        int owner = Rendezvous.rank(key, ports).getFirst();
        return nodes.get(ports.indexOf(owner));
    }
}
