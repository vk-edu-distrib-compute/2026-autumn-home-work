package company.vk.edu.distrib.compute.akravchenya.urlshortener;

import com.sun.net.httpserver.HttpExchange;
import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.NoSuchElementException;

/**
 * Проверка базовой аутентификации RFC 7617 на соответствие хранилищу пользователей.
 */
final class Authentication {

    private static final String BASIC_PREFIX = "Basic ";

    private final Dao<String> users;

    Authentication(Dao<String> users) {
        this.users = users;
    }

    /**
     * Проверка, чтобы в запросе содержались действительные базовые учетные данные сохраненного пользователя.
     *
     * @param exchange обмен, для которого проверяется заголовок {@code Authorization}
     * @throws UnauthorizedException если запрос не авторизован
     * @throws IOException           если хранилище пользователей не удалось прочитать
     */
    void require(HttpExchange exchange) throws IOException {
        var authorization = exchange.getRequestHeaders().getFirst("Authorization");
        if (authorization == null || !authorization.regionMatches(true, 0, BASIC_PREFIX, 0, BASIC_PREFIX.length())) {
            throw new UnauthorizedException("basic credentials are required");
        }
        var decoded = decodeCredentials(authorization.substring(BASIC_PREFIX.length()));
        var separator = decoded.indexOf(':');
        if (separator <= 0) {
            throw new UnauthorizedException("credentials must be 'username:password'");
        }
        var username = decoded.substring(0, separator);
        var suppliedPassword = decoded.substring(separator + 1);
        String expectedPassword;
        try {
            expectedPassword = users.get(username);
        } catch (NoSuchElementException exception) {
            throw new UnauthorizedException("unknown user: " + username, exception);
        }
        if (!suppliedPassword.equals(expectedPassword)) {
            throw new UnauthorizedException("invalid credentials");
        }
    }

    private static String decodeCredentials(String encoded) {
        try {
            return new String(Base64.getDecoder().decode(encoded), StandardCharsets.UTF_8);
        } catch (IllegalArgumentException exception) {
            throw new UnauthorizedException("malformed credentials", exception);
        }
    }
}
