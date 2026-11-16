package company.vk.edu.distrib.compute.robert.kv.sharding.strategy;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

public class RendezvousShardingStrategy<T> implements ShardingStrategy<T> {
    private static final int LEAST_SHARDS_COUNT_NUMBER = 1;

    private final int shardsCount;
    private final ThreadLocal<MessageDigest> digests = 
        ThreadLocal.withInitial(() -> {
            try {
                return MessageDigest.getInstance("SHA-256");
            } catch (NoSuchAlgorithmException e) {
                throw new IllegalStateException(e);
            }
        });
    private final List<byte[]> shardIdxBytes;
    private final Function<T, byte[]> toBytes;
    
    public RendezvousShardingStrategy(int inputShardsCount, Function<T, byte[]> inputToBytes) {
        if (inputShardsCount < LEAST_SHARDS_COUNT_NUMBER) {
            throw new IllegalArgumentException();
        }
        
        shardsCount = inputShardsCount;
        toBytes = inputToBytes;
        
        shardIdxBytes = new ArrayList<>();
        byte[] idxBytes;
        for (int i = 0; i < shardsCount; ++i) {
            idxBytes = ByteBuffer
                .allocate(Integer.BYTES)
                .putInt(i)
                .array();
            
            shardIdxBytes.add(idxBytes);
        }
    }

    @Override
    public int getShardIndex(T key) {
        if (key == null) {
            throw new IllegalArgumentException();
        }

        byte[] maxHash = null;
        byte[] curHash;
        byte[] keyBytes = toBytes.apply(key);
        int maxHashIdx = -1;
        MessageDigest digest = digests.get();
        int isGreater;

        for (int i = 0; i < shardsCount; ++i) {
            digest.update(shardIdxBytes.get(i));
            curHash = digest.digest(keyBytes);
            isGreater = Arrays.compareUnsigned(curHash, maxHash);

            if (maxHashIdx == -1 || isGreater > 0) {
                maxHash = curHash;
                maxHashIdx = i;
            }
        }

        return maxHashIdx;
    }
}
