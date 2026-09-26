package company.vk.edu.distrib.compute.vagifbaratov.urlshortener.handler;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.IOException;
import java.util.NoSuchElementException;

public class ErrorHandler implements HttpHandler {
    private final HttpHandler delegate;

    public ErrorHandler(HttpHandler delegate) {
        this.delegate = delegate;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        try {
            delegate.handle(exchange);
        } catch (NoSuchElementException e) {
            exchange.sendResponseHeaders(404, -1);
        } catch (IllegalArgumentException e) {
            exchange.sendResponseHeaders(422, -1);
        } catch (Exception e) {
            exchange.sendResponseHeaders(505, -1);
        } finally {
            exchange.close();
        }
    }
}
