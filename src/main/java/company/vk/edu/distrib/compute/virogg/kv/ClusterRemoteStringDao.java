package company.vk.edu.distrib.compute.virogg.kv;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

import company.vk.edu.distrib.compute.Dao;

final class ClusterRemoteStringDao implements Dao<String> {
    private final ConsistentHashRing ring;
    private final Map<Integer, RemoteStringDao> clients;

    ClusterRemoteStringDao(int... ports) {
        int[] clusterPorts = ports.clone();
        ring = new ConsistentHashRing(clusterPorts);
        clients = Arrays.stream(clusterPorts).boxed()
                .collect(Collectors.toUnmodifiableMap(Function.identity(), RemoteStringDao::new));
    }

    @Override
    public String get(String key) throws IOException {
        return client(key).get(key);
    }

    @Override
    public void upsert(String key, String value) throws IOException {
        client(key).upsert(key, value);
    }

    @Override
    public void delete(String key) throws IOException {
        client(key).delete(key);
    }

    @Override
    public void close() {
        for (RemoteStringDao client : clients.values()) {
            client.close();
        }
    }

    private RemoteStringDao client(String key) {
        return Objects.requireNonNull(clients.get(ring.portFor(key)));
    }
}
