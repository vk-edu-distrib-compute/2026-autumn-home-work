package company.vk.edu.distrib.compute.nickmish.urlshortener;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.NoSuchElementException;
import java.util.function.BooleanSupplier;

public final class UrlShortenerHandler implements HttpHandler {
    private static final String GET = "GET";
    private static final String POST = "POST";
    private static final String PUT = "PUT";
    private static final String DELETE = "DELETE";
    private static final String STATUS_PATH = "/v0/status";
    private static final String USERS_PATH = "/internal/users";
    private static final String LINKS_PATH = "/v0/links/";
    private static final String CONTENT_TYPE = "text/html; charset=utf-8";

    private final String shortLinkPrefix;
    private final LinkService links;
    private final UserService users;
    private final BasicAuthenticator auth;
    private final BooleanSupplier available;

    public UrlShortenerHandler(
            int port,
            LinkService links,
            UserService users,
            BasicAuthenticator auth,
            BooleanSupplier available) {
        this.shortLinkPrefix = "http://localhost:" + port + "/";
        this.links = links;
        this.users = users;
        this.auth = auth;
        this.available = available;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        try (exchange) {
            Response response = process(exchange);
            exchange.getResponseHeaders().set("Content-Type", CONTENT_TYPE);
            if (response.hasLocation()) {
                exchange.getResponseHeaders().set("Location", response.location());
            }
            if (response.body().isEmpty()) {
                exchange.sendResponseHeaders(response.status(), -1);
            } else {
                byte[] body = response.body().getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(response.status(), body.length);
                exchange.getResponseBody().write(body);
            }
        }
    }

    private Response process(HttpExchange exchange) {
        try {
            return route(exchange);
        } catch (NoSuchElementException e) {
            return new Response(404);
        } catch (IllegalArgumentException e) {
            return new Response(422);
        } catch (IOException e) {
            return new Response(503);
        }
    }

    private Response route(HttpExchange exchange) throws IOException {
        String path = exchange.getRequestURI().getRawPath();
        String method = exchange.getRequestMethod();

        if (STATUS_PATH.equals(path)) {
            return status(exchange, method);
        }
        if (USERS_PATH.equals(path)) {
            return registerUser(exchange, method);
        }

        boolean isRedirectPath = path.startsWith("/") && path.indexOf('/', 1) == -1;
        if (isRedirectPath && GET.equals(method)) {
            return redirect(path.substring(1));
        }

        if (!auth.authenticate(exchange.getRequestHeaders())) {
            exchange.getResponseHeaders().set("WWW-Authenticate", auth.getChallenge());
            return new Response(401);
        }

        return routeLinks(exchange, path, method, isRedirectPath);
    }

    private Response status(HttpExchange exchange, String method) {
        if (!GET.equals(method)) {
            return methodNotAllowed(exchange, "GET");
        }
        return available.getAsBoolean() ? new Response(200) : new Response(503);
    }

    private Response registerUser(HttpExchange exchange, String method) throws IOException {
        if (!POST.equals(method)) {
            return methodNotAllowed(exchange, "POST");
        }
        users.register(readBody(exchange));
        return new Response(200);
    }

    private Response redirect(String id) throws IOException {
        String longLink = links.get(id);
        return new Response(301, "", longLink);
    }

    private Response routeLinks(
            HttpExchange exchange,
            String path,
            String method,
            boolean isRedirectPath)
            throws IOException {
        if ("/v0/links".equals(path)) {
            if (POST.equals(method)) {
                String id = links.create(readBody(exchange));
                return new Response(201, shortLinkPrefix + id);
            }
            return methodNotAllowed(exchange, "POST");
        }
        if (path.startsWith(LINKS_PATH)) {
            String id = path.substring(LINKS_PATH.length());
            return accessLink(exchange, id, method);
        }
        if (isRedirectPath) {
            return methodNotAllowed(exchange, "GET");
        }
        return new Response(404);
    }

    private Response accessLink(HttpExchange exchange, String id, String method) throws IOException {
        return switch (method) {
            case GET -> new Response(200, links.get(id));
            case PUT -> {
                links.update(id, readBody(exchange));
                yield new Response(200);
            }
            case DELETE -> {
                links.delete(id);
                yield new Response(202);
            }
            default -> methodNotAllowed(exchange, "GET, PUT, DELETE");
        };
    }

    private static Response methodNotAllowed(HttpExchange exchange, String methods) {
        exchange.getResponseHeaders().set("Allow", methods);
        return new Response(405);
    }

    private static String readBody(HttpExchange exchange) throws IOException {
        byte[] bytes = exchange.getRequestBody().readAllBytes();
        return StandardCharsets.UTF_8.newDecoder().decode(ByteBuffer.wrap(bytes)).toString();
    }
}
