package company.vk.edu.distrib.compute.fedorkhokhryakov.kv;

import com.sun.net.httpserver.HttpExchange;

import java.io.Closeable;
import java.io.IOException;

public interface EntityHandler extends Closeable {
    void handle(HttpExchange exchange, String id) throws IOException;
}
