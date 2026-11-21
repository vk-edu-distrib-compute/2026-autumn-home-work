package company.vk.edu.distrib.compute.akravchenya.urlshortener;

import com.sun.net.httpserver.HttpExchange;
import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;

/**
 * Перенаправляет {@code GET /<id>} на сохраненную длинную ссылку, если идентификатор существует.
 */
final class RedirectHandler implements RequestHandler.ThrowingHandler {

    private static final String GET = "GET";

    private final Dao<String> links;

    RedirectHandler(Dao<String> links) {
        this.links = links;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        if (!GET.equals(exchange.getRequestMethod())) {
            RequestHandler.sendEmpty(exchange, 405);
            return;
        }
        var id = Urls.requireId(exchange.getRequestURI().getPath().substring(1));
        var longLink = links.get(id);
        exchange.getResponseHeaders().set("Location", longLink);
        RequestHandler.sendEmpty(exchange, 301);
    }
}
