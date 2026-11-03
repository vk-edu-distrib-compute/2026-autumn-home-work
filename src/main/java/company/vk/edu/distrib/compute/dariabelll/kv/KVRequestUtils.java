package company.vk.edu.distrib.compute.dariabelll.kv;

import org.jspecify.annotations.Nullable;

import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

final class KVRequestUtils {

    private static final String ID_QUERY_PARAMETER = "id=";
    private static final String ACK_QUERY_PARAMETER = "ack=";

    private KVRequestUtils() {
    }

    static @Nullable String extractKey(URI uri) {
        String query = uri.getRawQuery();
        if (query == null) {
            return null;
        }
        for (String param : query.split("&")) {
            if (param.startsWith(ID_QUERY_PARAMETER)) {
                String key = URLDecoder.decode(
                        param.substring(ID_QUERY_PARAMETER.length()), StandardCharsets.UTF_8
                );
                return key.isEmpty() ? null : key;
            }
        }
        return null;
    }

    static int extractAck(URI uri) {
        String query = uri.getRawQuery();
        if (query == null) {
            return 1;
        }
        for (String param : query.split("&")) {
            if ("ack".equals(param)) {
                throw new IllegalArgumentException("Missing ack value");
            }
            if (param.startsWith(ACK_QUERY_PARAMETER)) {
                String value = URLDecoder.decode(
                        param.substring(ACK_QUERY_PARAMETER.length()), StandardCharsets.UTF_8
                );
                return Integer.parseInt(value);
            }
        }
        return 1;
    }

    static List<String> getAllowedMethods(String path) {
        return switch (path) {
            case "/v0/status" -> List.of("GET");
            case "/v0/entity" -> List.of("GET", "PUT", "DELETE");
            default -> List.of();
        };
    }

    static @Nullable ReplicationConfig readReplicationConfig() {
        String cluster = System.getProperty("CLUSTER");
        if (cluster == null || cluster.isBlank()) {
            return null;
        }
        int[] ports = Arrays.stream(cluster.split(",", -1))
                .map(String::strip)
                .mapToInt(Integer::parseInt)
                .toArray();
        String factor = System.getProperty("REPLICATION_FACTOR");
        int replicationFactor = factor == null ? ports.length : Integer.parseInt(factor.strip());
        return new ReplicationConfig(ports, replicationFactor);
    }
}
