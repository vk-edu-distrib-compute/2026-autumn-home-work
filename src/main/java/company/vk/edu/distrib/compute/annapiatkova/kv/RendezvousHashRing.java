package company.vk.edu.distrib.compute.annapiatkova.kv;

import java.util.List;
import java.util.stream.IntStream;

class RendezvousHashRing {
    List<String> clusters;

    RendezvousHashRing(List<String> clusters) {
        this.clusters = clusters;
    }

    public int getClusterIndex(String key) {
        return IntStream.range(0, clusters.size())
            .boxed()
            .min((i, j) -> Integer.compare(hash(clusters.get(i), key), hash(clusters.get(j), key)))
            .get();
    }

    private int hash(String cluster, String key) {
        return (cluster + ":" + key).hashCode();
    }
}
