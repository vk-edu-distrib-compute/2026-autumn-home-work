package company.vk.edu.distrib.compute.netheer.kv;

import java.net.HttpURLConnection;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

final class ReplicaResponseCollector {
    private final String method;
    private final int ack;
    private final BlockingQueue<ReplicaResult> completed =
            new LinkedBlockingQueue<>();

    private int successful;
    private int remaining;
    private ReplicaResult selected =
            new ReplicaResult(HttpURLConnection.HTTP_NOT_FOUND, new byte[0]);

    ReplicaResponseCollector(String method, int ack) {
        this.method = method;
        this.ack = ack;
    }

    ReplicaResult collect(
            List<CompletableFuture<ReplicaResult>> requests,
            Duration timeout
    ) throws InterruptedException {
        remaining = requests.size();
        registerResponses(requests);

        long deadline = System.nanoTime() + timeout.toNanos();

        while (remaining > 0) {
            long timeLeft = deadline - System.nanoTime();
            if (timeLeft <= 0) {
                break;
            }

            ReplicaResult response =
                    completed.poll(timeLeft, TimeUnit.NANOSECONDS);
            if (response == null) {
                break;
            }

            acceptResponse(response);

            if (successful >= ack) {
                return selected;
            }
            if (successful + remaining < ack) {
                break;
            }
        }

        return unavailable();
    }

    private void registerResponses(
            List<CompletableFuture<ReplicaResult>> requests
    ) {
        for (CompletableFuture<ReplicaResult> request : requests) {
            request.whenComplete((result, error) ->
                    completed.offer(error == null ? result : unavailable())
            );
        }
    }

    private void acceptResponse(ReplicaResult response) {
        remaining--;

        if (!isSuccessful(response.statusCode())) {
            return;
        }

        successful++;
        if (!"GET".equals(method)
                || response.statusCode() == HttpURLConnection.HTTP_OK) {
            selected = response;
        }
    }

    private boolean isSuccessful(int statusCode) {
        return switch (method) {
            case "GET" ->
                    statusCode == HttpURLConnection.HTTP_OK
                            || statusCode == HttpURLConnection.HTTP_NOT_FOUND;
            case "PUT" -> statusCode == HttpURLConnection.HTTP_CREATED;
            case "DELETE" -> statusCode == HttpURLConnection.HTTP_ACCEPTED;
            default -> false;
        };
    }

    private static ReplicaResult unavailable() {
        return new ReplicaResult(
                HttpURLConnection.HTTP_UNAVAILABLE,
                new byte[0]
        );
    }
}
