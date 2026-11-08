package company.vk.edu.distrib.compute.sghdjsdfhgfj.kv;

import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.NoSuchElementException;

public class MyShardedRemoteDao implements Dao<String> {
    private final ShardManager shardManager;
    private final int ack;

    public MyShardedRemoteDao(int... ports) {
        this.shardManager = new ShardManager(ports);
        this.ack = 3;
    }

    @Override
    public String get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        MyRemoteDao dao = shardManager.determineShards(key)[0];
        return new String(dao.get(key, ack), StandardCharsets.UTF_8);
    }

    @Override
    public void upsert(String key, String value) throws IllegalArgumentException, IOException {
        MyRemoteDao dao = shardManager.determineShards(key)[0];
        dao.upsert(key, value.getBytes(StandardCharsets.UTF_8), ack);
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        MyRemoteDao dao = shardManager.determineShards(key)[0];
        dao.delete(key, ack);
    }

    @Override
    public void close() throws IOException {
        shardManager.close();
    }
}
