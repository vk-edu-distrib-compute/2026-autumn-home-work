package company.vk.edu.distrib.compute.sh4rrkyyyy.urlshortener;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.IOException;
import java.util.NoSuchElementException;

public class ErrorHandler implements HttpHandler {
    private final HttpHandler handler;

    public ErrorHandler(HttpHandler handler) {
        this.handler = handler;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        try {
            handler.handle(exchange);
        } catch (NoSuchElementException e) {
            HttpUtils.sendRsp(exchange, 404, e.getMessage());
        } catch (IllegalArgumentException e) {
            HttpUtils.sendRsp(exchange, 422, e.getMessage());
        } catch (Exception e) {
            HttpUtils.sendRsp(exchange, 500, e.getMessage());
        }
    }
}
