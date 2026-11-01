package company.vk.edu.distrib.compute.sh4rrkyyyy.common;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.IOException;
import java.util.NoSuchElementException;

public class ErrorHandler implements HttpHandler {
    private final HttpHandler handler;
    private final int invalidRequestStatus;

    public ErrorHandler(HttpHandler handler) {
        this(handler, HttpStatus.UNPROCESSABLE_ENTITY);
    }

    public ErrorHandler(HttpHandler handler, int invalidRequestStatus) {
        this.handler = handler;
        this.invalidRequestStatus = invalidRequestStatus;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        try {
            handler.handle(exchange);
        } catch (NoSuchElementException e) {
            HttpUtils.sendRsp(exchange, HttpStatus.NOT_FOUND, e.getMessage());
        } catch (IllegalArgumentException e) {
            HttpUtils.sendRsp(exchange, invalidRequestStatus, e.getMessage());
        } catch (Exception e) {
            HttpUtils.sendRsp(exchange, HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage());
        }
    }
}
