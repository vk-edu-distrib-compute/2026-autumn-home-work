package company.vk.edu.distrib.compute.sghdjsdfhgfj;

import com.sun.net.httpserver.HttpExchange;

import java.io.IOException;

public interface CustomHttpHandler {
    default void handleGet(HttpExchange xch) throws IOException, StatusCodeException {
        throw StatusCodeException.methodNotAllowed();
    }

    default void handlePost(HttpExchange xch) throws IOException, StatusCodeException {
        throw StatusCodeException.methodNotAllowed();
    }

    default void handlePut(HttpExchange xch) throws IOException, StatusCodeException {
        throw StatusCodeException.methodNotAllowed();
    }

    default void handleDelete(HttpExchange xch) throws IOException, StatusCodeException {
        throw StatusCodeException.methodNotAllowed();
    }
}
