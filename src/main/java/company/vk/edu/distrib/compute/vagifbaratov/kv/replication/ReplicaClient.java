package company.vk.edu.distrib.compute.vagifbaratov.kv.replication;

import company.vk.edu.distrib.compute.vagifbaratov.kv.ClusterConfig;

import java.io.IOException;

@FunctionalInterface
public interface ReplicaClient {
    ReplicaResponse execute(
            int targetPort,
            String method,
            String key,
            byte[] body,
            ClusterConfig config
    ) throws IOException, InterruptedException;
}
