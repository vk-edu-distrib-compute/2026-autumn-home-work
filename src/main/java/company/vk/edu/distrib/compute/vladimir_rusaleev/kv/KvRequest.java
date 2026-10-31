package company.vk.edu.distrib.compute.vladimir_rusaleev.kv;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.stream.IntStream;

record KvRequest(String key, int ack, int[] replicas, boolean configured) {
    private static final int MAX_PORT = 65535;

    static KvRequest parse(String query, int localPort) {
        String key = parameter(query, "id", "");
        if (key.isEmpty()) {
            throw new IllegalArgumentException("Empty id");
        }

        String description = System.getProperty("CLUSTER", System.getenv("CLUSTER"));
        int[] ports = description == null ? new int[]{localPort} : parsePorts(description, localPort);
        int factor = description == null ? 1 : replicationFactor(ports.length);

        int ack = Integer.parseInt(parameter(query, "ack", "1"));
        if (ack < 1 || ack > factor) {
            throw new IllegalArgumentException("Ack out of range");
        }

        int first = ClusterLinksDao.ownerIndex(key, ports);
        int[] replicas = IntStream.range(0, factor)
            .map(i -> ports[(first + i) % ports.length])
            .toArray();

        return new KvRequest(key, ack, replicas, description != null);
    }

    private static int[] parsePorts(String description, int localPort) {
        int[] ports = Arrays.stream(description.split(",", -1))
            .map(String::trim)
            .mapToInt(Integer::parseInt)
            .sorted()
            .toArray();

        for (int i = 0; i < ports.length; i++) {
            if (ports[i] < 1 || ports[i] > MAX_PORT) {
                throw new IllegalArgumentException("Invalid port");
            }
            if (i > 0 && ports[i] == ports[i - 1]) {
                throw new IllegalArgumentException("Duplicate port");
            }
        }

        if (Arrays.binarySearch(ports, localPort) < 0) {
            throw new IllegalArgumentException("Local node is missing from CLUSTER");
        }

        return ports;
    }

    private static int replicationFactor(int size) {
        String value = System.getProperty("REPLICATION_FACTOR",
            System.getenv().getOrDefault("REPLICATION_FACTOR", Integer.toString(size)));

        int factor = Integer.parseInt(value);
        if (factor < 1 || factor > size) {
            throw new IllegalArgumentException("Invalid replication factor");
        }

        return factor;
    }

    private static String parameter(String query, String name, String fallback) {
        if (query == null) {
            throw new IllegalArgumentException("Missing query");
        }

        String result = fallback;
        boolean found = false;

        for (String part : query.split("&", -1)) {
            String[] pair = part.split("=", 2);
            if (pair.length != 2) {
                throw new IllegalArgumentException("Missing parameter value");
            }

            if (name.equals(URLDecoder.decode(pair[0], StandardCharsets.UTF_8))) {
                if (found) {
                    throw new IllegalArgumentException("Duplicate parameter");
                }
                found = true;
                result = URLDecoder.decode(pair[1], StandardCharsets.UTF_8);
            }
        }

        return result;
    }
}
