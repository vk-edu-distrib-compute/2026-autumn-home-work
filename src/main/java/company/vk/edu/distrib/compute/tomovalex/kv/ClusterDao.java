package company.vk.edu.distrib.compute.tomovalex.kv;

import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ClusterDao implements Dao<String> {
    private final Map<Integer, RemoteDao> nodes = new ConcurrentHashMap<>();
    private final ReplicaSelector replicaSelector;

    public ClusterDao(int... ports) {
        this.replicaSelector = new ReplicaSelector(ports);
        replicaSelector.getPorts().stream()
            .map(RemoteDao::new)
            .forEach(node -> nodes.put(node.getPort(), node));
    }

    @Override
    public String get(String key) throws IOException {
        return selectNode(key).get(key);
    }

    @Override
    public void upsert(String key, String value) throws IOException {
        selectNode(key).upsert(key, value);
    }

    @Override
    public void delete(String key) throws IOException {
        selectNode(key).delete(key);
    }

    @Override
    public void close() {
        for (RemoteDao node : nodes.values()) {
            node.close();
        }
    }

    private RemoteDao selectNode(String key) {
        int port = replicaSelector.selectReplicas(key, 1).getFirst();
        return nodes.get(port);
    }

}
