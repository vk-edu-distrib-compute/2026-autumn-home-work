package company.vk.edu.distrib.compute.fedorkhokhryakov.urlshortener;

import com.sun.net.httpserver.HttpExchange;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.NoSuchElementException;

public class BasicAuthenticator {
    private final InMemoryUserDao userDao;

    public BasicAuthenticator(InMemoryUserDao userDao) {
        this.userDao = userDao;
    }

    public boolean authenticate(HttpExchange exchange) {
        String authorization = exchange.getRequestHeaders().getFirst("Authorization");

        if (authorization == null || !authorization.startsWith("Basic ")) {
            return false;
        }

        try {
            String encoded = authorization.substring("Basic ".length());
            String decoded = new String(
                Base64.getDecoder().decode(encoded),
                StandardCharsets.UTF_8
            );

            int separator = decoded.indexOf(':');

            if (separator <= 0) {
                return false;
            }

            String username = decoded.substring(0, separator);
            String password = decoded.substring(separator + 1);

            return password.equals(userDao.get(username));
        } catch (IllegalArgumentException | NoSuchElementException e) {
            return false;
        }
    }
}
