package company.vk.edu.distrib.compute.nickmish.urlshortener;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import company.vk.edu.distrib.compute.nickmish.HttpStatus;

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
    private static final String ALLOW_GET = "GET";
    private static final String ALLOW_POST = "POST";
    private static final String ALLOW_GET_PUT_DELETE = "GET, PUT, DELETE";

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
                exchange.sendResponseHeaders(response.statusCode(), -1);
            } else {
                byte[] body = response.body().getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(response.statusCode(), body.length);
                exchange.getResponseBody().write(body);
            }
        }
    }

    private Response process(HttpExchange exchange) {
        try {
            return route(exchange);
        } catch (NoSuchElementException e) {
            return new Response(HttpStatus.NOT_FOUND);
        } catch (IllegalArgumentException e) {
            return new Response(HttpStatus.UNPROCESSABLE_CONTENT);
        } catch (IOException e) {
            return new Response(HttpStatus.SERVICE_UNAVAILABLE);
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
            return new Response(HttpStatus.UNAUTHORIZED);
        }

        return routeLinks(exchange, path, method, isRedirectPath);
    }

    private Response status(HttpExchange exchange, String method) {
        if (!GET.equals(method)) {
            return methodNotAllowed(exchange, ALLOW_GET);
        }
        return available.getAsBoolean()
                ? new Response(HttpStatus.OK)
                : new Response(HttpStatus.SERVICE_UNAVAILABLE);
    }

    private Response registerUser(HttpExchange exchange, String method) throws IOException {
        if (!POST.equals(method)) {
            return methodNotAllowed(exchange, ALLOW_POST);
        }
        users.register(readBody(exchange));
        return new Response(HttpStatus.OK);
    }

    private Response redirect(String id) throws IOException {
        String longLink = links.get(id);
        return new Response(HttpStatus.MOVED_PERMANENTLY, "", longLink);
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
                return new Response(HttpStatus.CREATED, shortLinkPrefix + id);
            }
            return methodNotAllowed(exchange, ALLOW_POST);
        }
        if (path.startsWith(LINKS_PATH)) {
            String id = path.substring(LINKS_PATH.length());
            return accessLink(exchange, id, method);
        }
        if (isRedirectPath) {
            return methodNotAllowed(exchange, ALLOW_GET);
        }
        return new Response(HttpStatus.NOT_FOUND);
    }

    private Response accessLink(HttpExchange exchange, String id, String method) throws IOException {
        return switch (method) {
            case GET -> new Response(HttpStatus.OK, links.get(id));
            case PUT -> {
                links.update(id, readBody(exchange));
                yield new Response(HttpStatus.OK);
            }
            case DELETE -> {
                links.delete(id);
                yield new Response(HttpStatus.ACCEPTED);
            }
            default -> methodNotAllowed(exchange, ALLOW_GET_PUT_DELETE);
        };
    }

    private static Response methodNotAllowed(HttpExchange exchange, String methods) {
        exchange.getResponseHeaders().set("Allow", methods);
        return new Response(HttpStatus.METHOD_NOT_ALLOWED);
    }

    private static String readBody(HttpExchange exchange) throws IOException {
        byte[] bytes = exchange.getRequestBody().readAllBytes();
        return StandardCharsets.UTF_8.newDecoder().decode(ByteBuffer.wrap(bytes)).toString();
    }
}
