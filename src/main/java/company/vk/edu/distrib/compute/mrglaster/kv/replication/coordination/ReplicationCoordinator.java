package company.vk.edu.distrib.compute.mrglaster.kv.replication.coordination;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.mrglaster.kv.replication.config.ClusterConfig;
import company.vk.edu.distrib.compute.mrglaster.kv.replication.config.ClusterConfigReader;
import company.vk.edu.distrib.compute.mrglaster.kv.replication.replica.LocalReplicaExecutor;
import company.vk.edu.distrib.compute.mrglaster.kv.replication.replica.RemoteReplicaClient;
import company.vk.edu.distrib.compute.mrglaster.kv.replication.replica.ReplicaResult;
import company.vk.edu.distrib.compute.mrglaster.kv.replication.replica.ReplicaSelector;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;

public final class ReplicationCoordinator {

    private final int localPort;
    private final LocalReplicaExecutor localExecutor;
    private final RemoteReplicaClient remoteClient;

    public ReplicationCoordinator(int localPort, Dao<byte[]> localDao) {
        this.localPort = localPort;
        this.localExecutor = new LocalReplicaExecutor(localDao);
        this.remoteClient = new RemoteReplicaClient();
    }

    public int replicationFactor() {
        ClusterConfig cfg = ClusterConfigReader.read();
        return cfg == null ? 1 : cfg.replicationFactor();
    }

    public ReplicaResult execute(String method, String key, byte[] body, int ack) {
        ClusterConfig cfg = ClusterConfigReader.read();
        if (cfg == null) {
            return executeSingle(ack, method, key, body);
        }
        return executeReplicated(cfg, method, key, body, ack);
    }

    private ReplicaResult executeSingle(int ack, String method, String key, byte[] body) {
        if (ack > 1) {
            return ReplicaResult.UNAVAILABLE;
        }
        return localExecutor.execute(method, key, body);
    }

    private ReplicaResult executeReplicated(ClusterConfig cfg, String method, String key, byte[] body, int ack) {
        int[] replicas = ReplicaSelector.select(key, cfg);
        BlockingQueue<ReplicaResult> queue = new LinkedBlockingQueue<>();
        for (int targetPort : replicas) {
            dispatch(queue, method, key, body, targetPort);
        }
        return new QuorumCollector(method, ack, replicas.length, queue).collect();
    }

    private void dispatch(BlockingQueue<ReplicaResult> queue, String method, String key, byte[] body, int targetPort) {
        CompletableFuture<ReplicaResult> fut = (targetPort == localPort)
                ? CompletableFuture.completedFuture(localExecutor.execute(method, key, body))
                : remoteClient.send(method, key, body, targetPort);
        fut.thenAccept(queue::add);
    }

    public void close() {
        remoteClient.close();
    }
}
