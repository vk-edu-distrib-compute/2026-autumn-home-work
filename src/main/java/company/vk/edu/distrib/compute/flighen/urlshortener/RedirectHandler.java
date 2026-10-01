package company.vk.edu.distrib.compute.flighen.urlshortener;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import company.vk.edu.distrib.compute.Dao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.Objects;

public class RedirectHandler implements HttpHandler {
    private final Logger log =
            LoggerFactory.getLogger(RedirectHandler.class);

    private final Dao<String> dao;

    public RedirectHandler(Dao<String> dao) {
        this.dao = dao;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        final var method = exchange.getRequestMethod();

        try (exchange) {
            if (Objects.equals(method, "GET")) {

                String[] path = exchange.getRequestURI().getPath().split("/");

                String id = path[path.length - 1];

                if (id == null || !IdUtils.isValidId(id)) {
                    exchange.sendResponseHeaders(422, -1);
                    exchange.close();
                    return;
                }

                try {
                    String longLink = dao.get(id);

                    exchange.getResponseHeaders()
                            .add("Location", longLink);

                    exchange.sendResponseHeaders(301, -1);
                } catch (NoSuchElementException e) {
                    exchange.sendResponseHeaders(404, -1);
                } catch (IllegalArgumentException e) {
                    exchange.sendResponseHeaders(422, -1);
                }
            } else {
                exchange.sendResponseHeaders(403, -1);
            }
        } catch (IOException e) {
            if (log.isErrorEnabled()) {
                log.error(
                        "I/O error while handling {} {}",
                        exchange.getRequestMethod(),
                        exchange.getRequestURI(),
                        e
                );
            }

            throw e;
        }

        exchange.close();
    }
}
