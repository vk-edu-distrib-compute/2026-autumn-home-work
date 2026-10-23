package company.vk.edu.distrib.compute.miiishenka.urlshortener.controller;

import java.io.IOException;

import com.sun.net.httpserver.HttpExchange;
import company.vk.edu.distrib.compute.miiishenka.http.exception.HttpStatusException;

public class StatusController extends BaseUrlShortenerController {
    @Override
    public String getPath() {
        return "/v0/status";
    }

    @Override
    public void get(HttpExchange exchange) throws HttpStatusException, IOException {
        validateExactPath(exchange);
        exchange.sendResponseHeaders(200, 0);
    }
}
