package company.vk.edu.distrib.compute.vagifbaratov.kv.replication;

import company.vk.edu.distrib.compute.vagifbaratov.kv.ClusterConfig;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static company.vk.edu.distrib.compute.vagifbaratov.kv.replication.ReplicaUtils.successStatus;

public class ReplicaCoordinator implements AutoCloseable {
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(2);
    private static final int OK = 200;
    private static final int NOT_FOUND = 404;
    private static final int UNAVAILABLE = 503;

    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
    private final ReplicaClient replicaClient;

    public ReplicaCoordinator(ReplicaClient replicaClient) {
        this.replicaClient = replicaClient;
    }

    public ReplicaResponse execute(String method, String key, byte[] body, int ack, ClusterConfig cfg) {
        CompletionService<ReplicaResponse> responses = new ExecutorCompletionService<>(executor);

        int first = ownerIndex(key, cfg.ports());

        for (int i = 0; i < cfg.replicationFactor(); i++) {
            int target = cfg.ports()[(first + i) % cfg.ports().length];

            responses.submit(() -> replicaClient.execute(target, method, key, body, cfg));
        }

        try {
            return collectResponses(responses, method, ack, cfg.replicationFactor());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return new ReplicaResponse(UNAVAILABLE, new byte[0]);
        }
    }

    private ReplicaResponse collectResponses(
            CompletionService<ReplicaResponse> responses,
            String method,
            int ack,
            int replicas
    ) throws InterruptedException {
        int success = 0;
        ReplicaResponse result = new ReplicaResponse(
                "GET".equals(method) ? NOT_FOUND : successStatus(method),
                new byte[0]
        );

        long deadline = System.nanoTime() + REQUEST_TIMEOUT.toNanos();

        for (int remaining = replicas; remaining > 0; remaining--) {
            ReplicaResponse response = awaitResponse(responses, deadline);
            if (response == null) {
                break;
            }

            if (isAck(method, response.status())) {
                success++;

                if (response.status() == OK) {
                    result = response;
                }
            }

            if (success >= ack) {
                return result;
            }

            if (success + remaining - 1 < ack) {
                break;
            }
        }

        return new ReplicaResponse(UNAVAILABLE, new byte[0]);
    }

    private ReplicaResponse awaitResponse(
            CompletionService<ReplicaResponse> responses,
            long deadline
    ) throws InterruptedException {
        var completed = responses.poll(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
        if (completed == null) {
            return null;
        }

        try {
            return completed.get();
        } catch (ExecutionException e) {
            return new ReplicaResponse(UNAVAILABLE, new byte[0]);
        }
    }

    private boolean isAck(String method, int status) {
        return status == successStatus(method) || ("GET".equals(method) && status == NOT_FOUND);
    }

    private int ownerIndex(String key, int...ports) {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("Key must be non-empty.");
        }

        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException ex) {
            throw new IllegalStateException("SHA-256 is unavailable", ex);
        }

        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        long bestScore = 0;
        int selected = -1;

        for (int i = 0; i < ports.length; i++) {
            digest.update((byte) (ports[i] >> Byte.SIZE));
            digest.update((byte) ports[i]);

            long score = ByteBuffer.wrap(digest.digest(keyBytes)).getLong();
            if (i == 0 || Long.compareUnsigned(score, bestScore) > 0) {
                bestScore = score;
                selected = i;
            }
        }
        return selected;
    }

    @Override
    public void close() {
        executor.shutdownNow();
        executor.close();
    }
}
