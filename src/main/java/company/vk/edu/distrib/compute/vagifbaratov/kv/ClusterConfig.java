package company.vk.edu.distrib.compute.vagifbaratov.kv;

public record ClusterConfig(String host, int[] ports, int replicationFactor) {}
