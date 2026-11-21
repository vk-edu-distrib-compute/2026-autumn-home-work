package company.vk.edu.distrib.compute.netheer.kv;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;

record EntityParams(String key, int ack) {
    private static final int PARAMETER_PARTS = 2;

    static EntityParams parse(String query, int replicationFactor) {
        if (query == null) {
            throw new IllegalArgumentException("Missing query parameters");
        }

        Map<String, String> parameters = new ConcurrentHashMap<>();
        for (String parameter : query.split("&", -1)) {
            addParameter(parameters, parameter);
        }

        String key = parameters.get("id");
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("Missing or empty id");
        }

        int ack = parseAck(parameters.get("ack"), replicationFactor);
        return new EntityParams(key, ack);
    }

    private static void addParameter(
            Map<String, String> parameters,
            String parameter
    ) {
        String[] pair = parameter.split("=", PARAMETER_PARTS);
        if (pair.length != PARAMETER_PARTS) {
            throw new IllegalArgumentException("Missing = in query parameter");
        }

        String name = URLDecoder.decode(pair[0], StandardCharsets.UTF_8);
        String value = URLDecoder.decode(pair[1], StandardCharsets.UTF_8);

        if (!"id".equals(name) && !"ack".equals(name)) {
            throw new IllegalArgumentException("Unknown parameter: " + name);
        }

        if (parameters.containsKey(name)) {
            throw new IllegalArgumentException("Duplicate parameter: " + name);
        }
        parameters.put(name, value);
    }

    private static int parseAck(String text, int replicationFactor) {
        int ack = text == null ? 1 : Integer.parseInt(text);
        if (ack < 1 || ack > replicationFactor) {
            throw new IllegalArgumentException("Invalid ack");
        }
        return ack;
    }
}
