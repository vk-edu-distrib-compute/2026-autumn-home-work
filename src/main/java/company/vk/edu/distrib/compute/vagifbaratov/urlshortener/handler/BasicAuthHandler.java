package company.vk.edu.distrib.compute.vagifbaratov.urlshortener.handler;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import company.vk.edu.distrib.compute.Dao;

import javax.security.sasl.AuthenticationException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Locale;
import java.util.NoSuchElementException;
import java.util.Objects;

public class BasicAuthHandler implements HttpHandler {
    private static final String BASIC_PREFIX = "basic ";
    private static final String UNAUTHORIZED = "Unauthorized";
    private static final int CREDENTIALS_LENGTH = 2;
    private final HttpHandler delegate;
    private final Dao<String> credentialsDao;

    public BasicAuthHandler(HttpHandler delegate, Dao<String> credentialsDao) {
        this.delegate = delegate;
        this.credentialsDao = credentialsDao;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        final var authorization = exchange.getRequestHeaders().getFirst("Authorization");

        if (authorization == null
                || !authorization.toLowerCase(Locale.ROOT).startsWith(BASIC_PREFIX)) {
            throw new AuthenticationException(UNAUTHORIZED);
        }

        final var encoded = authorization.substring(BASIC_PREFIX.length());
        final var credentials = decode(encoded);

        if (credentials.length != CREDENTIALS_LENGTH || credentials[0].isEmpty()) {
            throw new AuthenticationException(UNAUTHORIZED);
        }
        if (!authenticate(credentials[0], credentials[1])) {
            throw new AuthenticationException(UNAUTHORIZED);
        }

        delegate.handle(exchange);
    }

    private String[] decode(String encoded) throws AuthenticationException {

        try {
            return new String(
                    Base64.getDecoder().decode(encoded),
                    StandardCharsets.UTF_8
            ).split(":", 2);
        } catch (IllegalArgumentException ex) {
            throw new AuthenticationException(UNAUTHORIZED, ex);
        }
    }

    private boolean authenticate(String user, String password) throws IOException {
        try {
            final var expectedPassword = credentialsDao.get(user);
            if (Objects.equals(expectedPassword, password)) {
                return true;
            }
        } catch (NoSuchElementException e) {
            return false;
        }
        return false;
    }
}
