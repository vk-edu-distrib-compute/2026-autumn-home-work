package company.vk.edu.distrib.compute.iizhukov.urlshortener.api.helpers;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

public abstract class BaseController implements HttpHandler {
    private final int port;
    private final Map<String, Function<Request, Response>> methods = Map.of(
            "GET", this::get,
            "POST", this::post,
            "PUT", this::put,
            "DELETE", this::delete
    );
    private final List<Middleware> middlewares;

    public BaseController(int port) {
        this.port = port;
        this.middlewares = List.of();
    }

    public BaseController(int port, List<Middleware> middlewares) {
        this.port = port;
        this.middlewares = middlewares;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        var request = Request.from(exchange);

        var method = methods.get(exchange.getRequestMethod());
        if (method == null) {
            throw new IOException("invalid method");
        }

        Response response;
        try {
            var handler = chain(method::apply, middlewares);
            response = handler.handle(request);

            if (response == null) {
                response = Response.builder().build();
            }

        } catch (IllegalArgumentException e) {
            response = Response.builder()
                    .setStatus(HttpStatus.UNPROCESSABLE_CONTENT)
                    .build();
        }

        makeExchange(exchange, response);
        exchange.close();
    }

    private void makeExchange(HttpExchange exchange, Response response) throws IOException {
        response.headers().forEach((k, v) -> {
            exchange.getResponseHeaders().set(k, v);
        });
        exchange.sendResponseHeaders(response.status(), response.length());

        try (var out = exchange.getResponseBody()) {
            out.write(response.content().getBytes(StandardCharsets.UTF_8));
        }
    }

    private static Handler chain(
            Handler handler,
            List<Middleware> middlewares
    ) {
        Handler result = handler;

        for (int i = middlewares.size() - 1; i >= 0; i--) {
            Middleware middleware = middlewares.get(i);
            Handler next = result;

            result = request -> middleware.handle(request, next);
        }

        return result;
    }

    protected int port() {
        return port;
    }

    public abstract String path();

    public abstract Response get(Request request);

    public abstract Response post(Request request);

    public abstract Response put(Request request);

    public abstract Response delete(Request request);
}
