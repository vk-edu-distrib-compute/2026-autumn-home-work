package company.vk.edu.distrib.compute.mrglaster.kv.replication.config;

public record ClusterConfig(int[] ports, int replicationFactor) {

    public int size() {
        return ports.length;
    }

    public int portAt(int index) {
        return ports[index];
    }
}
