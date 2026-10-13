package company.vk.edu.distrib.compute.robert.kv.sharding.strategy;

@FunctionalInterface
public interface ShardingStrategy<T> {
    int getShardIndex(T key);
}
