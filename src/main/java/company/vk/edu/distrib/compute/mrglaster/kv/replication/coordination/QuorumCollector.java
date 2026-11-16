package company.vk.edu.distrib.compute.mrglaster.kv.replication.coordination;

import company.vk.edu.distrib.compute.mrglaster.kv.replication.replica.ReplicaResult;
import company.vk.edu.distrib.compute.mrglaster.urlshortener.controller.enums.StatusCode;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public final class QuorumCollector {

    private static final long COLLECT_TIMEOUT_SECONDS = 2;

    private final String method;
    private final int ack;
    private final int replicas;
    private final BlockingQueue<ReplicaResult> queue;

    private int completed;
    private int confirmed;
    private byte[] value;

    public QuorumCollector(String method, int ack, int replicas, BlockingQueue<ReplicaResult> queue) {
        this.method = method;
        this.ack = ack;
        this.replicas = replicas;
        this.queue = queue;
    }

    public ReplicaResult collect() {
        try {
            while (shouldContinue()) {
                ReplicaResult r = queue.poll(COLLECT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                if (r == null) {
                    break;
                }
                accept(r);
                if (impossible()) {
                    break;
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return ReplicaResult.UNAVAILABLE;
        }
        return buildResponse();
    }

    private boolean shouldContinue() {
        return completed < replicas && confirmed < ack;
    }

    private boolean impossible() {
        return confirmed + (replicas - completed) < ack;
    }

    private void accept(ReplicaResult r) {
        completed++;
        if (!r.isConfirmation(method)) {
            return;
        }
        confirmed++;
        if (isGetOk(r)) {
            value = r.body();
        }
    }

    private boolean isGetOk(ReplicaResult r) {
        return "GET".equals(method) && r.status() == StatusCode.HTTP_OK.getStatusCode() && value == null;
    }

    private ReplicaResult buildResponse() {
        if (confirmed < ack) {
            return ReplicaResult.UNAVAILABLE;
        }
        return switch (method) {
            case "GET" -> value != null
                    ? new ReplicaResult(200, value)
                    : new ReplicaResult(404, new byte[0]);
            case "PUT" -> new ReplicaResult(201, new byte[0]);
            case "DELETE" -> new ReplicaResult(200, new byte[0]);
            default -> new ReplicaResult(500, new byte[0]);
        };
    }
}
