package company.vk.edu.distrib.compute.vagifbaratov.kv.replication;

final class ReplicaUtils {
    private static final int OK = 200;
    private static final int CREATED = 201;
    private static final int ACCEPTED = 202;

    private ReplicaUtils() {
    }

     static int successStatus(String method) {
        return switch (method) {
            case "GET" -> OK;
            case "PUT" -> CREATED;
            case "DELETE" -> ACCEPTED;
            default -> throw new IllegalArgumentException("Unsupported method");
        };
    }
}
