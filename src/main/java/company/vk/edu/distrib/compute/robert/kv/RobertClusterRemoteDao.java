package company.vk.edu.distrib.compute.robert.kv;

import java.io.IOException;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.Function;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.robert.kv.sharding.strategy.RendezvousShardingStrategy;
import company.vk.edu.distrib.compute.robert.kv.sharding.strategy.ShardingStrategy;

public class RobertClusterRemoteDao<T> implements Dao<T> {
    private final HttpClient client;
    private final List<RobertRemoteDao<T>> shards;
    private final ShardingStrategy<String> shardingStrategy;

    public RobertClusterRemoteDao(
        Function<T, byte[]> inputEncoder,
        Function<byte[], T> inputDecoder,
        int... ports
    ) {
        this(
            "http://localhost",
            new RendezvousShardingStrategy<>(
                ports.length,
                value -> value.getBytes(StandardCharsets.UTF_8)
            ),
            inputEncoder,
            inputDecoder,
            ports
        );
    }

    public RobertClusterRemoteDao(
        String inputBaseUrl,
        ShardingStrategy<String> inputShardingStrategy,
        Function<T, byte[]> inputEncoder,
        Function<byte[], T> inputDecoder,
        int... ports
    ) {
        if (inputBaseUrl == null || inputBaseUrl.isEmpty()
            || inputShardingStrategy == null
            || ports.length <= 0
        ) {
            throw new IllegalArgumentException();
        }
        Objects.requireNonNull(inputEncoder);
        Objects.requireNonNull(inputDecoder);
        shardingStrategy = inputShardingStrategy;
        client = HttpClient.newHttpClient();
        List<RobertRemoteDao<T>> remoteShards = new ArrayList<>(ports.length);
        for (int port : ports) {
            remoteShards.add(
                new RobertRemoteDao<>(
                    inputBaseUrl,
                    port,
                    client,
                    inputEncoder,
                    inputDecoder
                )
            );
        }
        shards = List.copyOf(remoteShards);
    }

    @Override
    public void close() throws IOException {
        client.close();
    }

    @Override
    public T get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        int shardIdx = shardingStrategy.getShardIndex(key);
        return shards.get(shardIdx).get(key);
    }

    @Override
    public void upsert(String key, T value) throws IllegalArgumentException, IOException {
        int shardIdx = shardingStrategy.getShardIndex(key);
        shards.get(shardIdx).upsert(key, value);
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        int shardIdx = shardingStrategy.getShardIndex(key);
        shards.get(shardIdx).delete(key);
    }
}
