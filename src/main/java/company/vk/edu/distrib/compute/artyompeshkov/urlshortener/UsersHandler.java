package company.vk.edu.distrib.compute.artyompeshkov.urlshortener;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;

import com.sun.net.httpserver.HttpExchange;
import company.vk.edu.distrib.compute.Dao;

import static company.vk.edu.distrib.compute.artyompeshkov.urlshortener.HttpUtils.NO_BODY;
import static company.vk.edu.distrib.compute.artyompeshkov.urlshortener.HttpUtils.sendMethodNotAllowed;

class UsersHandler extends BaseHandler {
    static final String PATH = "/internal/users";
    private final Dao<String> users;

    UsersHandler(Dao<String> users) {
        super();
        this.users = users;
    }

    @Override
    protected void doHandle(HttpExchange exchange) throws IOException {
        if (!POST.equals(exchange.getRequestMethod())) {
            sendMethodNotAllowed(exchange, POST);
            return;
        }
        String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
        User user = SimpleAuth.parseUser(body);
        users.upsert(user.name(), user.password());
        exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, NO_BODY);
    }
}
