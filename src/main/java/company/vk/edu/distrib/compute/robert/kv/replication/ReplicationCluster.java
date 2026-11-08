package company.vk.edu.distrib.compute.robert.kv.replication;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import company.vk.edu.distrib.compute.robert.kv.sharding.strategy.RendezvousShardingStrategy;
import company.vk.edu.distrib.compute.robert.kv.sharding.strategy.ShardingStrategy;

public class ReplicationCluster {
    private final String nodes;
    private final String factor;
    private final List<Integer> ports;
    private final int replicationFactor;
    private final ShardingStrategy<String> shardingStrategy;

    public ReplicationCluster(String inputNodes, String inputFactor) {
        nodes = inputNodes;
        factor = inputFactor;
        ports = Arrays.stream(nodes.split(",", -1))
            .map(String::strip)
            .map(Integer::valueOf)
            .toList();

        if (new HashSet<>(ports).size() != ports.size()) {
            throw new IllegalArgumentException();
        }
        for (int port : ports) {
            if (port <= 0 || port >= 65536) {
                throw new IllegalArgumentException();
            }
        }

        replicationFactor = factor.isBlank() ? ports.size() : Integer.parseInt(factor);
        if (replicationFactor < 1 || replicationFactor > ports.size()) {
            throw new IllegalArgumentException();
        }
        shardingStrategy = new RendezvousShardingStrategy<>(
            ports.size(),
            value -> value.getBytes(StandardCharsets.UTF_8)
        );
    }

    public boolean matches(String inputNodes, String inputFactor) {
        return nodes.equals(inputNodes) && factor.equals(inputFactor);
    }

    public int replicationFactor() {
        return replicationFactor;
    }

    public List<Integer> replicas(String key) {
        int firstIndex = shardingStrategy.getShardIndex(key);
        List<Integer> replicas = new ArrayList<>(replicationFactor);
        for (int i = 0; i < replicationFactor; ++i) {
            replicas.add(ports.get((firstIndex + i) % ports.size()));
        }
        return List.copyOf(replicas);
    }
}
