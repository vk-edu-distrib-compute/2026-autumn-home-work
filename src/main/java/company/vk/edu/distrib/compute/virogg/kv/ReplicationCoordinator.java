package company.vk.edu.distrib.compute.virogg.kv;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.virogg.http.HttpUtils;
import org.jspecify.annotations.Nullable;

final class ReplicationCoordinator implements AutoCloseable {
    private static final byte[] EMPTY = new byte[0];
    private static final Duration CONNECT_TIMEOUT = Duration.ofMillis(300);
    private static final Duration REQUEST_TIMEOUT = Duration.ofMillis(1000);
    private static final long WAIT_NANOS = TimeUnit.MILLISECONDS.toNanos(1500);
    private final int localPort;
    private final Dao<byte[]> dao;
    private final ExecutorService replicas = Executors.newVirtualThreadPerTaskExecutor();
    private final HttpClient client = HttpClient.newBuilder().connectTimeout(CONNECT_TIMEOUT).build();
    private final AtomicReference<@Nullable CachedConfiguration> cached = new AtomicReference<>();

    ReplicationCoordinator(int localPort, Dao<byte[]> dao) {
        this.localPort = localPort;
        this.dao = dao;
    }

    ReplicationConfig configuration() {
        String cluster = clusterSetting();
        String factor = System.getProperty("virogg.kv.replicationFactor");
        CachedConfiguration snapshot = cached.get();
        if (snapshot != null && Objects.equals(snapshot.cluster(), cluster)
                && Objects.equals(snapshot.factor(), factor)) {
            return snapshot.config();
        }
        ReplicationConfig config = ReplicationConfig.parse(localPort, cluster, factor);
        cached.set(new CachedConfiguration(cluster, factor, config));
        return config;
    }

    static @Nullable String clusterSetting() {
        String property = System.getProperty("CLUSTER");
        return property == null ? System.getenv("CLUSTER") : property;
    }

    ReplicaResponse execute(String method, String key, byte[] body, int ack, ReplicationConfig config) {
        if (ack < 1 || ack > config.replicationFactor()) {
            throw new IllegalArgumentException("Acks must fit the replication factor");
        }
        CompletionService<ReplicaResponse> completed = new ExecutorCompletionService<>(replicas);
        int[] ports = config.portsFor(key);
        try {
            for (int port : ports) {
                completed.submit(() -> invoke(port, method, key, body));
            }
            return collect(completed, method, ack, ports.length);
        } catch (RejectedExecutionException e) {
            return unavailable();
        }
    }

    private static ReplicaResponse collect(CompletionService<ReplicaResponse> completed,
                                            String method, int ack, int total) {
        long deadline = System.nanoTime() + WAIT_NANOS;
        int successes = 0;
        ReplicaResponse result = new ReplicaResponse(successStatus(method), EMPTY);
        try {
            for (int remaining = total; remaining > 0 && successes + remaining >= ack; remaining--) {
                Future<ReplicaResponse> future = completed.poll(deadline - System.nanoTime(), TimeUnit.NANOSECONDS);
                if (future == null) {
                    return unavailable();
                }
                ReplicaResponse response = completedResponse(future);
                if (successful(method, response.status())) {
                    successes++;
                    if (response.status() == HttpURLConnection.HTTP_OK) {
                        result = response;
                    }
                    if (successes >= ack) {
                        return result;
                    }
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return unavailable();
    }

    private static ReplicaResponse completedResponse(Future<ReplicaResponse> future) throws InterruptedException {
        try {
            return future.get();
        } catch (ExecutionException e) {
            return unavailable();
        }
    }

    private ReplicaResponse invoke(int port, String method, String key, byte[] body) {
        try {
            if (port == localPort) {
                return local(dao, method, key, body);
            }
            HttpRequest request = KvRequestFactory.entity(port, key, REQUEST_TIMEOUT)
                    .header(KvApiConstants.REPLICA_HEADER, "true")
                    .method(method, HttpRequest.BodyPublishers.ofByteArray(body)).build();
            return remote(request);
        } catch (IOException e) {
            return unavailable();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return unavailable();
        }
    }

    private ReplicaResponse remote(HttpRequest request) throws InterruptedException {
        CompletableFuture<HttpResponse<byte[]>> pending =
                client.sendAsync(request, HttpResponse.BodyHandlers.ofByteArray());
        try {
            HttpResponse<byte[]> response = pending.get(REQUEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            return new ReplicaResponse(response.statusCode(), response.body());
        } catch (ExecutionException | TimeoutException e) {
            return unavailable();
        } finally {
            pending.cancel(true);
        }
    }

    static ReplicaResponse local(Dao<byte[]> dao, String method, String key, byte[] body) throws IOException {
        switch (method) {
            case HttpUtils.GET -> {
                try {
                    return new ReplicaResponse(HttpURLConnection.HTTP_OK, dao.get(key));
                } catch (NoSuchElementException e) {
                    return new ReplicaResponse(HttpURLConnection.HTTP_NOT_FOUND, EMPTY);
                }
            }
            case HttpUtils.PUT -> dao.upsert(key, body);
            case HttpUtils.DELETE -> dao.delete(key);
            default -> throw new IllegalArgumentException("Unsupported method " + method);
        }
        return new ReplicaResponse(successStatus(method), EMPTY);
    }

    private static boolean successful(String method, int status) {
        return status == successStatus(method) || HttpUtils.GET.equals(method) && status == HttpURLConnection.HTTP_OK;
    }

    private static int successStatus(String method) {
        return switch (method) {
            case HttpUtils.GET -> HttpURLConnection.HTTP_NOT_FOUND;
            case HttpUtils.PUT -> HttpURLConnection.HTTP_CREATED;
            case HttpUtils.DELETE -> HttpURLConnection.HTTP_ACCEPTED;
            default -> throw new IllegalArgumentException("Unsupported method " + method);
        };
    }

    private static ReplicaResponse unavailable() {
        return new ReplicaResponse(HttpURLConnection.HTTP_UNAVAILABLE, EMPTY);
    }

    @Override
    public void close() {
        try (HttpClient _ = client) {
            replicas.close();
        }
    }

    private record CachedConfiguration(@Nullable String cluster, @Nullable String factor, ReplicationConfig config) {
    }
}
