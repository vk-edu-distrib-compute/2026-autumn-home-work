package company.vk.edu.distrib.compute.nickmish.urlshortener;

import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

public final class BasicAuthenticator {
    private static final String CHALLENGE = "Basic realm=\"urlshortener\", charset=\"UTF-8\"";
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
        if (!header.startsWith("Basic ")) {
            return false;
        }
        String base64 = header.substring(6);
        String decoded;
        try {
            byte[] bytes = Base64.getDecoder().decode(base64);
            decoded = StandardCharsets.UTF_8.newDecoder().decode(ByteBuffer.wrap(bytes)).toString();
        } catch (IllegalArgumentException e) {
            return false;
        }
        int sep = decoded.indexOf(':');
        if (sep < 0) {
            return false;
        }
        String username = decoded.substring(0, sep);
        String password = decoded.substring(sep + 1);
        try {
            String storedPassword = users.get(username);
            return storedPassword.equals(password);
        } catch (NoSuchElementException e) {
            return false;
        }
    }

    public String getChallenge() {
        return CHALLENGE;
    }
}
