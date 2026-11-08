package company.vk.edu.distrib.compute.sghdjsdfhgfj.kv;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Random;

public class ShardManager {
    private final int shardCount;
    private int replicationFactor;
    private final Shard[] shards;

    public ShardManager(int... ports) {
        shardCount = ports.length;
        shards = new Shard[shardCount];
        replicationFactor = 1;

        Random random = new Random(6769);
        List<Shard> shardList = Arrays.stream(ports).mapToObj(port ->
                new Shard(new MyRemoteDao(port), random.nextLong())
        ).toList();
        shardList.toArray(shards);

        Arrays.sort(shards, Comparator.comparingLong(Shard::hash));
    }

    public void close() throws IOException {
        for (Shard shard : shards) {
            shard.dao().close();
        }
    }

    public int getShardCount() {
        return shardCount;
    }

    public void setReplicationFactor(int replicationFactor) {
        this.replicationFactor = replicationFactor;
    }

    public MyRemoteDao[] determineShards(String key) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hashBytes = digest.digest(key.getBytes(StandardCharsets.UTF_8));
            long hash = 0;
            for (int i = 0; i < 8; i++) {
                long x = hashBytes[i + 3];
                hash += x << (i * 8);
            }

            int startIndex = 0;
            for (int i = 0; i < shardCount; i++) {
                if (shards[i].hash() > hash) {
                    startIndex = i;
                    break;
                }
            }

            MyRemoteDao[] daos = new MyRemoteDao[replicationFactor];
            for (int i = 0; i < replicationFactor; i++) {
                daos[i] = shards[(i + startIndex) % shardCount].dao();
            }
            return daos;
        } catch (NoSuchAlgorithmException e) {
            // this can never happen
            return new MyRemoteDao[0];
        }
    }
}
