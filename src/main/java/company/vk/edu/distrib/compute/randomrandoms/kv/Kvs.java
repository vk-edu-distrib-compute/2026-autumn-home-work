package company.vk.edu.distrib.compute.randomrandoms.kv;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.kv.KVService;
import company.vk.edu.distrib.compute.randomrandoms.util.MethodConstants;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.file.Paths;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.Executors;

import static company.vk.edu.distrib.compute.randomrandoms.util.HandlerConstants.codeAndText;
import static company.vk.edu.distrib.compute.randomrandoms.util.HandlerConstants.justCode;

public class Kvs implements KVService {
    private final FileDao dao;

    private final HttpServer server;

    private final HttpHandler status = justCode(200);

    public static final String ENTITY = "/v0/entity";
    public static final String STATUS = "/v0/status";
    public static final String QUERY = "id=";
    public static final int INCORRECT_KEY_CODE = 400;
    public static final int NOT_FOUND_CODE = 404;
    public static final int FOUND_CODE = 200;
    public static final int DELETED_CODE = 202;
    public static final int PUT_CODE = 201;

    public Kvs(int port) throws IOException {
        String file = String.format("/tmp/kv-%d", port);
        dao = new FileDao(Paths.get(file));
        server = HttpServer.create();
        server.setExecutor(Executors.newFixedThreadPool(8));
        server.bind(new InetSocketAddress("localhost", port), 0);
        server.createContext(STATUS, status);
        server.createContext(ENTITY, this::entity);
    }

    private void entity(HttpExchange exc) throws IOException {
        try {
            String key;
            try {
                key = parseUri(exc.getRequestURI()).orElseThrow();
            } catch (NoSuchElementException e) {
                justCode(422).handle(exc);
                return;
            }
            if (key.isEmpty()) {
                justCode(INCORRECT_KEY_CODE).handle(exc);
                return;
            }
            if (MethodConstants.DELETE.equals(exc.getRequestMethod())) {
                dao.delete(key);
                justCode(DELETED_CODE).handle(exc);
                return;
            }
            if (MethodConstants.GET.equals(exc.getRequestMethod())) {
                try {
                    var value = dao.get(key);
                    codeAndText(FOUND_CODE, value).handle(exc);
                } catch (NoSuchElementException e) {
                    justCode(NOT_FOUND_CODE).handle(exc);
                }
                return;
            }
            if (MethodConstants.PUT.equals(exc.getRequestMethod())) {
                var value = exc.getRequestBody().readAllBytes();
                dao.upsert(key, value);
                justCode(PUT_CODE).handle(exc);
                return;
            }
            justCode(422).handle(exc);
        } catch (Exception e) {
            justCode(500).handle(exc);
        }
    }

    private Optional<String> parseUri(URI uri) {
        var query = uri.getQuery();
        if (!query.startsWith(QUERY)) {
            return Optional.empty();
        }
        return Optional.of(query.substring(QUERY.length()));
    }

    @Override
    public void start() {
        server.start();
    }

    @Override
    public void stop() {
        server.stop(1);
    }
}
