package company.vk.edu.distrib.compute.kl1dd.kv;

public class ReplicaSelector {
    private long hashingScore(String key, int port) {
        long score = Integer.toUnsignedLong((key + ":" + port).hashCode());

        for (int i = 0; i < key.length(); i++) {
            score ^= (key.charAt(i));
            score *= 53;
        }

        score ^= port;
        score *= 53;
        return score;
    }

    int selectFirstReplica(String key, int... clusterPorts) {
        int bestPort = clusterPorts[0];
        long bestScore = hashingScore(key, clusterPorts[0]);

        for (int i = 1; i < clusterPorts.length; i++) {
            long currentScore = hashingScore(key, clusterPorts[i]);
            if (Long.compareUnsigned(currentScore, bestScore) > 0) {
                bestPort = clusterPorts[i];
                bestScore = currentScore;
            }
        }

        return bestPort;
    }

    int[] selectReplicas(String key, int replicationFactor, int... clusterPorts) {
        if (replicationFactor < 1 || replicationFactor > clusterPorts.length) {
            throw new IllegalArgumentException();
        }

        int firstReplica = selectFirstReplica(key, clusterPorts);
        int firstIndex = 0;
        for (int i = 0; i < clusterPorts.length; i++) {
            if (clusterPorts[i] == firstReplica) {
                firstIndex = i;
                break;
            }
        }

        int[] replicas = new int[replicationFactor];
        int firstPartLength = Math.min(replicationFactor, clusterPorts.length - firstIndex);
        System.arraycopy(clusterPorts, firstIndex, replicas, 0, firstPartLength);

        int remaining = replicationFactor - firstPartLength;
        if (remaining > 0) {
            System.arraycopy(clusterPorts, 0, replicas, firstPartLength, remaining);
        }

        return replicas;
    }
}
