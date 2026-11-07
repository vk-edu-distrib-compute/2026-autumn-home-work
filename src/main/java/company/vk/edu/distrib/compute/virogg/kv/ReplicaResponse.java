package company.vk.edu.distrib.compute.virogg.kv;

import java.util.Objects;

record ReplicaResponse(int status, byte[] body) {
    ReplicaResponse {
        Objects.requireNonNull(body);
    }
}
