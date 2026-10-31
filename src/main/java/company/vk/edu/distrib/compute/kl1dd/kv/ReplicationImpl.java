package company.vk.edu.distrib.compute.kl1dd.kv;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class ReplicationImpl {
    private static final int HTTP_OK = 200;
    private static final int HTTP_NOT_FOUND = 404;
    private static final int HTTP_UNAVAILABLE = 503;

    static final class GetReplicaResult {
        private final int statusCode;
        private final byte[] value;

        GetReplicaResult(int statusCode, byte[] value) {
            this.statusCode = statusCode;

            this.value = copyValue(value);
        }

        public int getStatusCode() {
            return statusCode;
        }

        public byte[] getValue() {
            return copyValue(value);
        }
    }

    private static byte[] copyValue(byte[] value) {
        if (value == null) {
            return new byte[0];
        }
        return value.clone();
    }

    boolean waitForAck(List<CompletableFuture<Boolean>> futures, int initialSuccess, int ack) {
        if (initialSuccess >= ack) {
            return true;
        }
        if (futures.isEmpty()) {
            return false;
        }

        AtomicInteger successCount = new AtomicInteger(initialSuccess);
        AtomicInteger completedCount = new AtomicInteger();
        CompletableFuture<Boolean> result = new CompletableFuture<>();
        for (CompletableFuture<Boolean> future : futures) {
            future.thenAccept(success -> {
                if (success && successCount.incrementAndGet() >= ack) {
                    result.complete(true);
                }

                if (completedCount.incrementAndGet() == futures.size()
                        && successCount.get() < ack) {
                    result.complete(false);
                }
            });
        }

        return result.join();
    }

    GetReplicaResult waitForGetAck(List<CompletableFuture<GetReplicaResult>> futures,
                                                       GetReplicaResult localResult, int ack) {
        int initialResponses = 0;
        byte[] initialValue = null;
        if (localResult.getStatusCode() == HTTP_OK) {
            initialResponses = 1;
            initialValue = localResult.getValue();
        } else if (localResult.getStatusCode() == HTTP_NOT_FOUND) {
            initialResponses = 1;
        }

        AtomicInteger responseCount = new AtomicInteger(initialResponses);
        AtomicInteger completedCount = new AtomicInteger();
        AtomicReference<byte[]> value = new AtomicReference<>(initialValue);
        GetReplicaResult initialChecksResult = initialChecks(responseCount.get(), value.get(), ack, futures.isEmpty());
        if (initialChecksResult != null) {
            return initialChecksResult;
        }

        CompletableFuture<GetReplicaResult> result = new CompletableFuture<>();
        for (CompletableFuture<GetReplicaResult> future : futures) {
            future.thenAccept(replicaResult -> {
                processReplicaResponse(replicaResult, responseCount, value, result, ack);

                if (completedCount.incrementAndGet() == futures.size()) {
                    result.complete(buildResult(responseCount.get(), value.get(), ack));
                }
            });
        }

        return result.join();
    }

    GetReplicaResult buildResult(int responseCount, byte[] value, int ack) {
        if (responseCount < ack) {
            return new GetReplicaResult(HTTP_UNAVAILABLE, null);
        }
        if (value != null) {
            return new GetReplicaResult(HTTP_OK, value);
        }
        return new GetReplicaResult(HTTP_NOT_FOUND, null);
    }

    private GetReplicaResult initialChecks(int responseCount, byte[] value, int ack, boolean futuresEmpty) {
        if (responseCount >= ack && value != null) {
            return new GetReplicaResult(HTTP_OK, value);
        }
        if (futuresEmpty) {
            return buildResult(responseCount, value, ack);
        }

        return null;
    }

    private void processReplicaResponse(GetReplicaResult replicaResult, AtomicInteger responseCount,
                                        AtomicReference<byte[]> value, CompletableFuture<GetReplicaResult> result,
                                        int ack) {
        int status = replicaResult.getStatusCode();
        if (status == HTTP_OK) {
            value.compareAndSet(null, replicaResult.getValue());

            if (responseCount.incrementAndGet() >= ack) {
                result.complete(new GetReplicaResult(HTTP_OK, value.get()));
            }
        } else if (status == HTTP_NOT_FOUND) {
            responseCount.incrementAndGet();
        }
    }
}
