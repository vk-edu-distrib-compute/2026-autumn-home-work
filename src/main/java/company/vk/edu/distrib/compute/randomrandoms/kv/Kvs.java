package company.vk.edu.distrib.compute.randomrandoms.kv;

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
    private final FileDao dao = new FileDao(Paths.get(FILE));

    private final HttpServer server;

    private final HttpHandler status = justCode(200);

    private final HttpHandler entity = exc -> {
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
    };

    public static final String ENTITY = "/v0/entity";
    public static final String STATUS = "/v0/status";
    public static final int INCORRECT_KEY_CODE = 400;
    public static final int NOT_FOUND_CODE = 404;
    public static final int FOUND_CODE = 200;
    public static final int DELETED_CODE = 202;
    public static final int PUT_CODE = 201;

    private static final String FILE = "/tmp/kv";

    public Kvs(int port) throws IOException {
        server = HttpServer.create();
        server.setExecutor(Executors.newFixedThreadPool(8));
        server.bind(new InetSocketAddress("localhost", port), 0);
        server.createContext(STATUS, status);
        server.createContext(ENTITY, entity);
    }

    private Optional<String> parseUri(URI uri) {
        var path = uri.getPath();
        if (!path.startsWith(ENTITY)) {
            return Optional.empty();
        }
        return Optional.of(path.substring(ENTITY.length() + 1));
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
