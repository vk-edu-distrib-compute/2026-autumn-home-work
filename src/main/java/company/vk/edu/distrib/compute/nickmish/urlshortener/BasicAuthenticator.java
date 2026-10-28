package company.vk.edu.distrib.compute.nickmish.urlshortener;

import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

public final class BasicAuthenticator {
    private static final String CHALLENGE = "Basic realm=\"urlshortener\", charset=\"UTF-8\"";
    private static final String BASIC_PREFIX = "Basic ";
    private static final int BASIC_PREFIX_LENGTH = 6;
    private static final char SEPARATOR = ':';
    private final Dao<String> users;

    public BasicAuthenticator(Dao<String> users) {
        this.users = users;
    }

    public boolean authenticate(Map<String, List<String>> headers) throws IOException {
        List<String> values = headers.get("Authorization");
        if (values == null || values.size() != 1) {
            return false;
        }
        String header = values.getFirst();
        if (!header.startsWith(BASIC_PREFIX)) {
            return false;
        }
        String base64 = header.substring(BASIC_PREFIX_LENGTH);
        String decoded;
        try {
            byte[] bytes = Base64.getDecoder().decode(base64);
            decoded = StandardCharsets.UTF_8.newDecoder().decode(ByteBuffer.wrap(bytes)).toString();
        } catch (IllegalArgumentException e) {
            return false;
        }
        int sep = decoded.indexOf(SEPARATOR);
        if (sep < 0) {
            return false;
        }
        String username = decoded.substring(0, sep);
        String password = decoded.substring(sep + 1);
        try {
            String storedPassword = users.get(username);
            return MessageDigest.isEqual(
                    storedPassword.getBytes(StandardCharsets.UTF_8),
                    password.getBytes(StandardCharsets.UTF_8));
        } catch (NoSuchElementException e) {
            return false;
        }
    }

    public String getChallenge() {
        return CHALLENGE;
    }
}
