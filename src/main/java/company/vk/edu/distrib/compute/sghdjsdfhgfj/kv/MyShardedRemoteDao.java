package company.vk.edu.distrib.compute.sghdjsdfhgfj.kv;

import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

public class MyShardedRemoteDao implements Dao<String> {
    private final int shardCount;
    private final Shard[] shards;

    public MyShardedRemoteDao(int... ports) {
        shardCount = ports.length;
        shards = new Shard[shardCount];

        Random random = new Random(6769);
        List<Shard> shardList = Arrays.stream(ports).mapToObj(port ->
                new Shard(new MyRemoteDao(port), random.nextLong())
        ).toList();
        shardList.toArray(shards);

        Arrays.sort(shards, Comparator.comparingLong(Shard::hash));
    }

    @Override
    public String get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        MyRemoteDao dao = determineShard(key);
        return dao.get(key);
    }

    @Override
    public void upsert(String key, String value) throws IllegalArgumentException, IOException {
        MyRemoteDao dao = determineShard(key);
        dao.upsert(key, value);
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        MyRemoteDao dao = determineShard(key);
        dao.delete(key);
    }

    @Override
    public void close() throws IOException {
        for (Shard shard : shards) {
            shard.dao().close();
        }
    }

    private MyRemoteDao determineShard(String key) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hashBytes = digest.digest(key.getBytes(StandardCharsets.UTF_8));
            long hash = 0;
            for (int i = 0; i < 8; i++) {
                long x = hashBytes[i + 3];
                hash += x << (i * 8);
            }

            for (int i = 0; i < shardCount; i++) {
                if (shards[i].hash() > hash) {
                    return shards[i].dao();
                }
            }
            return shards[0].dao();
        } catch (NoSuchAlgorithmException e) {
            // this can never happen
            return shards[0].dao();
        }
    }
}
