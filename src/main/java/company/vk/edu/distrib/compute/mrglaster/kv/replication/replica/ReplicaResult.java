package company.vk.edu.distrib.compute.mrglaster.kv.replication.replica;

public record ReplicaResult(int status, byte[] body) {
    public static final ReplicaResult UNAVAILABLE = new ReplicaResult(503, new byte[0]);

    public boolean isConfirmation(String method) {
        return switch (method) {
            case "GET" -> status == 200 || status == 404;
            case "PUT" -> status == 201;
            case "DELETE" -> status == 202;
            default -> false;
        };
    }
}
