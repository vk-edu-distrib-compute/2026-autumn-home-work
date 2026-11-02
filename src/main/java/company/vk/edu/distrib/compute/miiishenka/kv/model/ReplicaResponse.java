package company.vk.edu.distrib.compute.miiishenka.kv.model;

public record ReplicaResponse(int statusCode, byte[] responseBody) {
    public static ReplicaResponse failed() {
        return new ReplicaResponse(500, null);
    }
}
