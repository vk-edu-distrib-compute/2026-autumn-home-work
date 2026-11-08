package company.vk.edu.distrib.compute.sghdjsdfhgfj.kv;

import com.sun.net.httpserver.HttpExchange;
import company.vk.edu.distrib.compute.sghdjsdfhgfj.CustomHttpHandler;

import java.io.IOException;

class StatusHandler implements CustomHttpHandler {
    @Override
    public void handleGet(HttpExchange xch) throws IOException {
        xch.sendResponseHeaders(200, 0);
    }
}
