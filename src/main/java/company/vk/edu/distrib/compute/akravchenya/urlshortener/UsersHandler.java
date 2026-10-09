package company.vk.edu.distrib.compute.akravchenya.urlshortener;

import com.sun.net.httpserver.HttpExchange;
import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;

/**
 * Создает или обновляет пользователя на {@code POST /internal/users}.
 */
final class UsersHandler implements RequestHandler.ThrowingHandler {

    private static final String POST = "POST";

    private final Dao<String> users;

    UsersHandler(Dao<String> users) {
        this.users = users;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        if (!POST.equals(exchange.getRequestMethod())) {
            RequestHandler.sendEmpty(exchange, 405);
            return;
        }
        var body = Urls.readBody(exchange);
        var separator = body.indexOf(':');
        if (separator <= 0) {
            throw new IllegalArgumentException("credentials must be 'username:password'");
        }
        users.upsert(body.substring(0, separator), body.substring(separator + 1));
        RequestHandler.sendEmpty(exchange, 200);
    }
}
