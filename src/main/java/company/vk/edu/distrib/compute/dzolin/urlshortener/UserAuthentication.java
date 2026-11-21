package company.vk.edu.distrib.compute.dzolin.urlshortener;

import com.sun.net.httpserver.HttpExchange;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.NoSuchElementException;

final class UserAuthentication implements AutoCloseable {
    private static final String INVALID_CREDENTIALS = "invalid credentials";

    private final PersistentDao userDao;

    UserAuthentication(PersistentDao userDao) {
        this.userDao = userDao;
    }

    void createUser(HttpExchange exchange) throws IOException {
        var credentialsStr = new String(exchange.getRequestBody().readAllBytes());
        var credentials = parseCredentials(credentialsStr);
        userDao.upsert(credentials.username, credentials.password);
        exchange.sendResponseHeaders(200, -1);
    }

    void check(HttpExchange exchange) {
        var authorization = exchange.getRequestHeaders().get("Authorization").getFirst();
        if (authorization == null || !authorization.regionMatches(true, 0, "Basic ", 0, 6)) {
            throw new UnauthorizedException(INVALID_CREDENTIALS);
        }

        var encodedCredentials = authorization.substring(6);
        String decodedCredentials;
        try {
            decodedCredentials = new String(Base64.getDecoder().decode(encodedCredentials), StandardCharsets.UTF_8);
        } catch (IllegalArgumentException exception) {
            throw new UnauthorizedException(INVALID_CREDENTIALS, exception);
        }
        var credentials = parseCredentials(decodedCredentials);

        String actualPassword;
        try {
            actualPassword = userDao.get(credentials.username);
        } catch (NoSuchElementException exception) {
            throw new UnauthorizedException(INVALID_CREDENTIALS, exception);
        }
        if (!credentials.password.equals(actualPassword)) {
            throw new UnauthorizedException(INVALID_CREDENTIALS);
        }
    }

    private Credentials parseCredentials(String credentialsStr) {
        int separator = credentialsStr.indexOf(':');
        if (separator <= 0) {
            throw new UnauthorizedException(INVALID_CREDENTIALS);
        }
        return new Credentials(credentialsStr.substring(0, separator), credentialsStr.substring(separator + 1));
    }

    @Override
    public void close() throws IOException {
        userDao.close();
    }

    private record Credentials(String username, String password) {
    }
}
