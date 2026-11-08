package company.vk.edu.distrib.compute.robert.kv.replication;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.ReentrantLock;

import company.vk.edu.distrib.compute.robert.api.models.HttpStatus;
import company.vk.edu.distrib.compute.robert.api.models.Response;

public class ReplicationQuorum {
    private final int ack;
    private final int successStatus;
    private final CompletableFuture<Response> result = new CompletableFuture<>();
    private final ReentrantLock lock = new ReentrantLock();
    private int remaining;
    private int confirmations;
    private Response response;

    public ReplicationQuorum(int inputAck, int replicasCount, int inputSuccessStatus) {
        ack = inputAck;
        remaining = replicasCount;
        successStatus = inputSuccessStatus;
        response = Response.builder()
            .setStatus(successStatus)
            .build();
    }

    public CompletableFuture<Response> result() {
        return result;
    }

    public void add(Response replicaResponse) {
        lock.lock();
        try {
            if (result.isDone()) {
                return;
            }

            --remaining;
            if (isConfirmation(replicaResponse.status())) {
                ++confirmations;
                if (replicaResponse.status() == HttpStatus.OK.code()) {
                    response = replicaResponse;
                }
            }

            if (confirmations >= ack) {
                result.complete(response);
            } else if (confirmations + remaining < ack) {
                result.complete(Response.builder()
                    .setStatus(HttpStatus.SERVICE_UNAVAILABLE.code())
                    .build());
            }
        } finally {
            lock.unlock();
        }
    }

    private boolean isConfirmation(int status) {
        if (successStatus == HttpStatus.NOT_FOUND.code()) {
            return status == HttpStatus.OK.code() || status == HttpStatus.NOT_FOUND.code();
        }
        if (successStatus == HttpStatus.OK.code()) {
            return status == HttpStatus.ACCEPTED.code();
        }
        return status == successStatus;
    }
}
