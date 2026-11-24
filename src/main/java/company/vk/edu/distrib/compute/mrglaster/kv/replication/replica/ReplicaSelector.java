package company.vk.edu.distrib.compute.mrglaster.kv.replication.replica;

import company.vk.edu.distrib.compute.mrglaster.kv.replication.config.ClusterConfig;

public final class ReplicaSelector {

    private ReplicaSelector() {
    }

    public static int[] select(String key, ClusterConfig cfg) {
        int start = startShard(key, cfg);
        int[] result = new int[cfg.replicationFactor()];
        for (int i = 0; i < result.length; i++) {
            result[i] = cfg.portAt((start + i) % cfg.size());
        }
        return result;
    }

    private static int startShard(String key, ClusterConfig cfg) {
        int shardNumber = 0;
        int maxHash = Integer.MIN_VALUE;
        for (int i = 0; i < cfg.size(); i++) {
            int hash = String.format("%d : %s", i, key).hashCode();
            if (hash > maxHash) {
                maxHash = hash;
                shardNumber = i;
            }
        }
        return shardNumber;
    }
}
