package company.vk.edu.distrib.compute.sghdjsdfhgfj.kv;

import com.sun.net.httpserver.HttpExchange;
import company.vk.edu.distrib.compute.sghdjsdfhgfj.CustomHttpHandler;
import company.vk.edu.distrib.compute.sghdjsdfhgfj.PersistentDao;
import company.vk.edu.distrib.compute.sghdjsdfhgfj.StatusCodeException;

import java.io.IOException;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

class StorageHandler implements CustomHttpHandler {
    private final PersistentDao storage;

    public StorageHandler(PersistentDao storage) {
        this.storage = storage;
    }

    @Override
    public void handleGet(HttpExchange xch) throws IOException, StatusCodeException {
        String id = getId(xch);
        if (storage.containsKey(id)) {
            byte[] value = storage.get(id);
            xch.sendResponseHeaders(200, 0);
            xch.getResponseBody().write(value);
        } else {
            throw StatusCodeException.notFound();
        }
    }

    @Override
    public void handlePut(HttpExchange xch) throws IOException, StatusCodeException {
        String id = getId(xch);
        byte[] value = xch.getRequestBody().readAllBytes();
        storage.upsert(id, value);
        xch.sendResponseHeaders(201, 0);
    }

    @Override
    public void handleDelete(HttpExchange xch) throws IOException, StatusCodeException {
        String id = getId(xch);
        storage.delete(id);
        xch.sendResponseHeaders(202, 0);
    }

    private Map<String, String> splitQuery(URI url) {
        // pmd is forcing me to use ConcurrentHashMap for a local variable for some reason
        Map<String, String> queryPairs = new ConcurrentHashMap<>();
        String query = url.getQuery();
        if (query == null) {
            return queryPairs;
        }
        String[] pairs = query.split("&");
        for (String pair : pairs) {
            int idx = pair.indexOf('=');
            queryPairs.put(
                    URLDecoder.decode(pair.substring(0, idx), StandardCharsets.UTF_8),
                    URLDecoder.decode(pair.substring(idx + 1), StandardCharsets.UTF_8)
            );
        }
        return queryPairs;
    }

    private String getId(HttpExchange xch) throws StatusCodeException {
        Map<String, String> params = splitQuery(xch.getRequestURI());
        if (!params.containsKey("id")) {
            throw StatusCodeException.badRequest();
        }
        String id = params.get("id");
        if (id.isEmpty()) {
            throw StatusCodeException.badRequest();
        }
        return id;
    }
}
