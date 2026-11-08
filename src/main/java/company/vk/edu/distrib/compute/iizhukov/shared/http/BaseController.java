package company.vk.edu.distrib.compute.iizhukov.shared.http;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import company.vk.edu.distrib.compute.Dao;
import org.jspecify.annotations.Nullable;

public abstract class BaseController<T> {
    @Nullable
    private Dao<T> dao;

    private final int port;
    private final Map<String, Function<Request, Response>> methods = Map.of(
            "GET", this::get,
            "POST", this::post,
            "PUT", this::put,
            "DELETE", this::delete
    );
    private final List<Middleware> middlewares;

    protected BaseController(int port) {
        this(port, List.of());
    }

    protected BaseController(int port, List<Middleware> middlewares) {
        this.port = port;
        this.middlewares = List.copyOf(middlewares);
    }

    public void setDao(Dao<T> dao) {
        this.dao = dao;
    }

    protected Dao<T> dao() {
        return Objects.requireNonNull(dao);
    }

    public HttpHandler handler(List<Middleware> globalMiddlewares) {
        var pipeline = new ArrayList<>(globalMiddlewares);
        pipeline.addAll(middlewares);
        var allMiddlewares = List.copyOf(pipeline);
        return exchange -> handle(exchange, allMiddlewares);
    }

    private void handle(HttpExchange exchange, List<Middleware> allMiddlewares) throws IOException {
        try (exchange) {
            var request = Request.from(exchange);
            var response = chain(this::route, allMiddlewares).handle(request);
            write(exchange, response);
        }
    }

    private Response route(Request request) {
        if (!matches(request)) {
            return Response.builder()
                    .setStatus(HttpStatus.NOT_FOUND)
                    .build();
        }

        return methods.getOrDefault(request.method(), value -> methodNotAllowed()).apply(request);
    }

    private static void write(HttpExchange exchange, Response response) throws IOException {
        response.headers().forEach(exchange.getResponseHeaders()::set);
        exchange.sendResponseHeaders(response.status(), response.length());

        try (var output = exchange.getResponseBody()) {
            output.write(response.content());
        }
    }

    private static Handler chain(Handler handler, List<Middleware> middlewares) {
        Handler result = handler;

        for (int i = middlewares.size() - 1; i >= 0; i--) {
            result = middlewares.get(i).apply(result);
        }

        return result;
    }

    protected boolean matches(Request request) {
        return request.path().startsWith(path());
    }

    protected int port() {
        return port;
    }

    private static Response methodNotAllowed() {
        return Response.builder()
                .setStatus(HttpStatus.METHOD_NOT_ALLOWED)
                .build();
    }

    public abstract String path();

    public Response get(Request request) {
        return methodNotAllowed();
    }

    public Response post(Request request) {
        return methodNotAllowed();
    }

    public Response put(Request request) {
        return methodNotAllowed();
    }

    public Response delete(Request request) {
        return methodNotAllowed();
    }
}
