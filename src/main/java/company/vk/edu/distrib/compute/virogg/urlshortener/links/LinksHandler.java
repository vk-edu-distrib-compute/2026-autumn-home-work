package company.vk.edu.distrib.compute.virogg.urlshortener.links;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.virogg.http.HttpUtils;
import org.jspecify.annotations.Nullable;

final class LinksHandler implements HttpHandler {
    static final String BASE_PATH = "/v0/links";
    private static final char PATH_SEPARATOR = '/';
    private static final String ITEM_PREFIX = BASE_PATH + PATH_SEPARATOR;
    private static final int ID_LENGTH = 10;
    private static final Pattern ID_PATTERN = Pattern.compile("[A-Za-z0-9]{" + ID_LENGTH + "}");
    private static final Set<String> ITEM_METHODS = Set.of(HttpUtils.GET, HttpUtils.PUT, HttpUtils.DELETE);

    private final Dao<String> links;
    private final String shortLinkPrefix;
    private final ReentrantLock writeLock = new ReentrantLock();

    LinksHandler(Dao<String> links, int port) {
        this.links = links;
        this.shortLinkPrefix = "http://localhost:" + port + PATH_SEPARATOR;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        String path = exchange.getRequestURI().getPath();
        String method = exchange.getRequestMethod();
        if (BASE_PATH.equals(path)) {
            if (HttpUtils.POST.equals(method)) {
                create(exchange);
            } else {
                HttpUtils.sendEmpty(exchange, HttpURLConnection.HTTP_BAD_METHOD);
            }
            return;
        }
        if (!path.startsWith(ITEM_PREFIX)) {
            HttpUtils.sendEmpty(exchange, HttpURLConnection.HTTP_NOT_FOUND);
            return;
        }
        if (!ITEM_METHODS.contains(method)) {
            HttpUtils.sendEmpty(exchange, HttpURLConnection.HTTP_BAD_METHOD);
            return;
        }
        String id = path.substring(ITEM_PREFIX.length());
        if (!isValidId(id)) {
            HttpUtils.sendEmpty(exchange, HttpUtils.HTTP_UNPROCESSABLE_CONTENT);
            return;
        }
        switch (method) {
            case HttpUtils.GET -> get(exchange, id);
            case HttpUtils.PUT -> update(exchange, id);
            default -> {
                remove(id);
                HttpUtils.sendEmpty(exchange, HttpURLConnection.HTTP_ACCEPTED);
            }
        }
    }

    public void redirect(HttpExchange exchange) throws IOException {
        if (!HttpUtils.GET.equals(exchange.getRequestMethod())) {
            HttpUtils.sendEmpty(exchange, HttpURLConnection.HTTP_BAD_METHOD);
            return;
        }
        String id = exchange.getRequestURI().getPath().substring(1);
        if (id.isEmpty() || id.indexOf(PATH_SEPARATOR) >= 0) {
            HttpUtils.sendEmpty(exchange, HttpURLConnection.HTTP_NOT_FOUND);
            return;
        }
        if (!isValidId(id)) {
            HttpUtils.sendEmpty(exchange, HttpUtils.HTTP_UNPROCESSABLE_CONTENT);
            return;
        }
        String link = find(id);
        if (link == null) {
            HttpUtils.sendEmpty(exchange, HttpURLConnection.HTTP_NOT_FOUND);
            return;
        }
        URI target = LinkUrlUtils.toAsciiUri(link);
        exchange.getResponseHeaders().set("Location", target == null ? link : target.toASCIIString());
        HttpUtils.sendEmpty(exchange, HttpURLConnection.HTTP_MOVED_PERM);
    }

    private void create(HttpExchange exchange) throws IOException {
        String link = readValidLink(exchange);
        if (link != null) {
            HttpUtils.sendText(exchange, HttpURLConnection.HTTP_CREATED, shortLinkPrefix + storeWithNewId(link));
        }
    }

    private void get(HttpExchange exchange, String id) throws IOException {
        String link = find(id);
        if (link == null) {
            HttpUtils.sendEmpty(exchange, HttpURLConnection.HTTP_NOT_FOUND);
            return;
        }
        HttpUtils.sendText(exchange, HttpURLConnection.HTTP_OK, link);
    }

    private void update(HttpExchange exchange, String id) throws IOException {
        String link = readValidLink(exchange);
        if (link != null) {
            boolean updated = replaceExisting(id, link);
            HttpUtils.sendEmpty(exchange, updated ? HttpURLConnection.HTTP_OK : HttpURLConnection.HTTP_NOT_FOUND);
        }
    }

    private @Nullable String readValidLink(HttpExchange exchange) throws IOException {
        String link = HttpUtils.readBody(exchange).strip();
        if (isValidLink(link)) {
            return link;
        }
        HttpUtils.sendEmpty(exchange, HttpUtils.HTTP_UNPROCESSABLE_CONTENT);
        return null;
    }

    private String storeWithNewId(String link) throws IOException {
        writeLock.lock();
        try {
            String id = newId();
            while (find(id) != null) {
                id = newId();
            }
            links.upsert(id, link);
            return id;
        } finally {
            writeLock.unlock();
        }
    }

    private boolean replaceExisting(String id, String link) throws IOException {
        writeLock.lock();
        try {
            if (find(id) == null) {
                return false;
            }
            links.upsert(id, link);
            return true;
        } finally {
            writeLock.unlock();
        }
    }

    private void remove(String id) throws IOException {
        writeLock.lock();
        try {
            links.delete(id);
        } finally {
            writeLock.unlock();
        }
    }

    private @Nullable String find(String id) throws IOException {
        try {
            return links.get(id);
        } catch (NoSuchElementException e) {
            return null;
        }
    }

    private static String newId() {
        return UUID.randomUUID().toString().replace("-", "").substring(0, ID_LENGTH);
    }

    private static boolean isValidId(String id) {
        return ID_PATTERN.matcher(id).matches();
    }

    private static boolean isValidLink(String link) {
        return LinkUrlUtils.toAsciiUri(link) != null;
    }
}
