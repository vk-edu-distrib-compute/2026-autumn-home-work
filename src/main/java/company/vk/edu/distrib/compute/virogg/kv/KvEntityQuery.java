package company.vk.edu.distrib.compute.virogg.kv;

import java.net.URLDecoder;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.jspecify.annotations.Nullable;

record KvEntityQuery(String key, int ack) {
    private static final int MIN_REPLICA_COUNT = 1;

    static KvEntityQuery parse(@Nullable String rawQuery, int replicationFactor) {
        if (rawQuery == null) {
            throw new IllegalArgumentException("Missing id");
        }
        if (replicationFactor < MIN_REPLICA_COUNT) {
            throw new IllegalArgumentException("Replication factor must be positive");
        }
        Map<String, String> parameters = parseParameters(rawQuery);
        String key = parameters.get(KvApiConstants.ID_PARAMETER);
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("Missing or empty id");
        }
        String acks = parameters.get("ack");
        int ack = acks == null ? MIN_REPLICA_COUNT : Integer.parseInt(acks);
        if (ack < MIN_REPLICA_COUNT || ack > replicationFactor) {
            throw new IllegalArgumentException("Ack count must be less or equal to the replication factor");
        }
        return new KvEntityQuery(key, ack);
    }

    private static Map<String, String> parseParameters(String rawQuery) {
        Map<String, String> parameters = new ConcurrentHashMap<>();
        for (String parameter : rawQuery.split("&", -1)) {
            int separator = parameter.indexOf('=');
            String name = decode(separator < 0 ? parameter : parameter.substring(0, separator));
            if (!KvApiConstants.ID_PARAMETER.equals(name) && !"ack".equals(name)) {
                continue;
            }
            if (separator < 0) {
                throw new IllegalArgumentException("Missing parameter value");
            }
            String value = decode(parameter.substring(separator + 1));
            if (parameters.put(name, value) != null) {
                throw new IllegalArgumentException("Duplicate parameter " + name);
            }
        }
        return parameters;
    }

    private static String decode(String encoded) {
        String bytes = new String(encoded.getBytes(StandardCharsets.UTF_8), StandardCharsets.ISO_8859_1);
        byte[] decoded = URLDecoder.decode(bytes, StandardCharsets.ISO_8859_1)
                .getBytes(StandardCharsets.ISO_8859_1);
        try {
            return StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(decoded)).toString();
        } catch (CharacterCodingException e) {
            throw new IllegalArgumentException("Invalid UTF-8 encoding", e);
        }
    }
}
