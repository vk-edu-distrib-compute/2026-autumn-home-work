package company.vk.edu.distrib.compute.annapiatkova.kv;

import java.net.http.HttpRequest;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.ArrayList;
import java.net.http.HttpResponse;
import java.net.http.HttpClient;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;

class RequestCoordinator {
    static final String REPLICA_PREFIX = "/v0/replica?";
    static final String ID_PREFIX = "id=";
    static final String CLUSTER_PROPERTY = "CLUSTER";
    static final int HTTP_OK = 200;
    static final int HTTP_CREATED = 201;
    static final int HTTP_ACCEPTED = 202;

    private RendezvousHashRing hashRing;
    private List<Integer> clusterPorts;
    private int replicationFactor;
    private final HttpClient client = HttpClient.newHttpClient();

    static class Result {
        int acks;
        byte[] value;
    }

    public boolean validateAck(int ack) {
        if (this.hashRing == null) {
            initialiseHashRing();
        }
        return ack >= 1 && ack <= this.replicationFactor;
    }

    CompletableFuture<HttpResponse<byte[]>> sendGet(int port, String id) {
        return this.client
            .sendAsync(this.get(port, id), HttpResponse.BodyHandlers.ofByteArray());
    }

    CompletableFuture<HttpResponse<byte[]>> sendPut(int port, String id, byte[] data) {
        return this.client
            .sendAsync(this.put(port, id, data), HttpResponse.BodyHandlers.ofByteArray());
    }

    CompletableFuture<HttpResponse<byte[]>> sendDelete(int port, String id) {
        return this.client
            .sendAsync(this.delete(port, id), HttpResponse.BodyHandlers.ofByteArray());
    }

    static Result handleGetResponses(List<CompletableFuture<HttpResponse<byte[]>>> responses, int repliesCount) {
        Result res = new Result();
        res.acks = repliesCount;
        for (CompletableFuture<HttpResponse<byte[]>> responseFuture : responses) {
            try {
                HttpResponse<byte[]> response = responseFuture.get(100, TimeUnit.MILLISECONDS);
                res.acks += 1;
                if (response.statusCode() != HTTP_OK) {
                    continue;
                }
                byte[] body = response.body();
                if (body != null) {
                    res.value = body;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (ExecutionException | TimeoutException e) {
                continue;
            }
        }
        return res;
    }

    static int handlePutResponses(List<CompletableFuture<HttpResponse<byte[]>>> responses) {
        int repliesCount = 0;
        for (CompletableFuture<HttpResponse<byte[]>> responseFuture : responses) {
            try {
                HttpResponse<byte[]> response = responseFuture.get(100, TimeUnit.MILLISECONDS);
                if (response.statusCode() == HTTP_CREATED) {
                    repliesCount += 1;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (ExecutionException | TimeoutException e) {
                continue;
            }
        }
        return repliesCount;
    }

    static int handleDeleteResponses(List<CompletableFuture<HttpResponse<byte[]>>> responses) {
        int repliesCount = 0;
        for (CompletableFuture<HttpResponse<byte[]>> responseFuture : responses) {
            try {
                HttpResponse<byte[]> response = responseFuture.get(100, TimeUnit.MILLISECONDS);
                if (response.statusCode() == HTTP_ACCEPTED) {
                    repliesCount += 1;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (ExecutionException | TimeoutException e) {
                continue;
            }
        }
        return repliesCount;
    }

    private HttpRequest get(int port, String id) {
        return HttpRequest.newBuilder()
            .uri(URI.create("http://localhost:" + port + REPLICA_PREFIX + ID_PREFIX + id))
            .timeout(Duration.ofMillis(100))
            .GET()
            .build();
    }

    private HttpRequest put(int port, String id, byte[] data) {
        return HttpRequest.newBuilder()
            .uri(URI.create("http://localhost:" + port + REPLICA_PREFIX + ID_PREFIX + id))
            .timeout(Duration.ofMillis(100))
            .PUT(HttpRequest.BodyPublishers.ofByteArray(data))
            .build();
    }

    private HttpRequest delete(int port, String id) {
        return HttpRequest.newBuilder()
            .uri(URI.create("http://localhost:" + port + REPLICA_PREFIX + ID_PREFIX + id))
            .timeout(Duration.ofMillis(100))
            .DELETE()
            .build();
    }

    List<Integer> getReplicaPorts(String key) {
        if (this.hashRing == null) {
            initialiseHashRing();
        }
        int index = this.hashRing.getClusterIndex(key);
        List<Integer> res = new ArrayList<>(this.replicationFactor);
        for (int i = 0; i < this.replicationFactor; i++) {
            res.add(this.clusterPorts.get((index + i) % this.clusterPorts.size()));
        }
        return res;
    }

    private void initialiseHashRing() {
        this.clusterPorts = getClusterPorts();
        this.replicationFactor = this.clusterPorts.size();
        List<String> clusters = new ArrayList<>(this.clusterPorts.size());
        for (int port : this.clusterPorts) {
            clusters.add("cluster-" + port);
        }
        this.hashRing = new RendezvousHashRing(clusters);
    }

    private List<Integer> getClusterPorts() {
        String cluster = System.getProperty(CLUSTER_PROPERTY);
        String[] ports = cluster.split(",");
        List<Integer> res = new ArrayList<>(ports.length);
        for (String port : ports) {
            res.add(Integer.parseInt(port));
        }
        return res;
    }
}
