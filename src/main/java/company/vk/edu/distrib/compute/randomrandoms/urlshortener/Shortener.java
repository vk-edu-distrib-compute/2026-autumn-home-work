package company.vk.edu.distrib.compute.randomrandoms.urlshortener;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.randomrandoms.kv.RamDao;
import company.vk.edu.distrib.compute.randomrandoms.util.MethodConstants;
import company.vk.edu.distrib.compute.urlshortener.UrlShortenerService;

import java.io.IOException;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static company.vk.edu.distrib.compute.randomrandoms.util.HandlerConstants.codeAndText;
import static company.vk.edu.distrib.compute.randomrandoms.util.HandlerConstants.justCode;

public class Shortener implements UrlShortenerService {
    private final HttpServer server;
    private Dao<String> links;
    private final RamDao<String> auth = new RamDao<>();
    private final Random rand;
    int port;
    boolean done;

    private final HttpHandler health = exc -> {
        if (!Objects.equals(exc.getRequestMethod(), "GET")) {
            justCode(422).handle(exc);
            return;
        }
        exc.sendResponseHeaders(200, 0);
        exc.close();
    };

    private final HttpHandler linksId = exc -> {
        final String linksPrefix = "/v0/links/";
        if (!checkAuth(exc)) {
            return;
        }
        var path = exc.getRequestURI().getPath();
        if (path.length() < linksPrefix.length()) {
            if (!MethodConstants.POST.equals(exc.getRequestMethod())) {
                justCode(422).handle(exc);
                return;
            }
            var link = new String(exc.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            if (!validLink(link)) {
                justCode(422).handle(exc);
                return;
            }
            String newId;
            while (true) {
                newId = generateId();
                try {
                    links.get(newId);
                } catch (NoSuchElementException e) {
                    links.upsert(newId, link);
                    break;
                }
            }
            var shortLink = "http://localhost:%d/%s".formatted(port, newId);
            codeAndText(201, shortLink.getBytes(StandardCharsets.UTF_8)).handle(exc);
            exc.close();
        }
        var id = path.substring("/v0/links/".length());
        if (!validId(id)) {
            justCode(422).handle(exc);
            return;
        }
        String origin = "";
        if (!MethodConstants.DELETE.equals(exc.getRequestMethod())) {
            try {
                origin = links.get(id);
            } catch (NoSuchElementException e) {
                justCode(404).handle(exc);
                return;
            }
        }
        if (MethodConstants.GET.equals(exc.getRequestMethod())) {
            codeAndText(200, origin.getBytes(StandardCharsets.UTF_8)).handle(exc);
            return;
        }
        if (MethodConstants.PUT.equals(exc.getRequestMethod())) {
            String newOrigin = new String(exc.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            if (!validLink(newOrigin)) {
                justCode(422).handle(exc);
                return;
            }
            links.upsert(id, newOrigin);
            justCode(200).handle(exc);
            return;
        }
        if (MethodConstants.DELETE.equals(exc.getRequestMethod())) {
            links.delete(id);
            justCode(202).handle(exc);
            return;
        }
        justCode(422).handle(exc);
    };

    private final HttpHandler redirect = exc -> {
        if (!MethodConstants.GET.equals(exc.getRequestMethod())) {
            justCode(422).handle(exc);
            return;
        }
        var id = exc.getRequestURI().getPath().substring(1);
        if (!validId(id)) {
            justCode(422).handle(exc);
            return;
        }
        String link;
        try {
            link = links.get(id);
        } catch (NoSuchElementException e) {
            justCode(404).handle(exc);
            return;
        }
        exc.getResponseHeaders().add("Location", link);
        exc.sendResponseHeaders(301, 0);
        exc.close();
    };

    private final HttpHandler internalUsers = exc -> {
        if (!MethodConstants.POST.equals(exc.getRequestMethod())) {
            justCode(422).handle(exc);
            return;
        }
        var creds = splitCreds(new String(exc.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
        try {
            var unwrappedCreds = creds.orElseThrow();
            auth.upsert(unwrappedCreds.uname, unwrappedCreds.pass);
            justCode(200).handle(exc);
        } catch (Exception e) {
            justCode(422).handle(exc);
        }
    };

    private record Creds(String uname, String pass) {
    }

    public Shortener(int port) throws IOException {
        server = HttpServer.create();
        server.bind(new InetSocketAddress("localhost", port), 0);
        server.createContext("/v0/status", health);
        server.createContext("/v0/links", linksId);
        server.createContext("/internal/users", internalUsers);
        server.createContext("/", redirect);
        rand = new Random();
        links = new RamDao<>();
        this.port = port;
        done = false;
    }

    @Override
    public void start() {
        server.start();
        done = true;
    }

    @Override
    public void stop() {
        server.stop(1);
        done = true;
    }

    @Override
    public void setLinksDao(Dao<String> dao) {
        if (done) {
            throw new IllegalStateException();
        }
        links = dao;
    }

    private String generateId() {
        final int base = 10;
        var ans = new StringBuilder();

        for (int i = 0; i < 10; ++i) {
            int rndv = rand.nextInt(10 + 26 * 2);
            char c;
            if (rndv < base) {
                c = (char) ('0' + rndv);
            } else if (rndv < 10 + 26) {
                c = (char) ('a' + rndv - 10);
            } else {
                c = (char) ('A' + rndv - 10 - 26);
            }
            ans.append(c);
        }

        return ans.toString();
    }

    private Boolean validId(String id) {
        return id.length() == 10 && id.chars().allMatch(c -> (c >= '0' && c <= '9')
                || (c >= 'a' && c <= 'z')
                || (c >= 'A' && c <= 'Z')
        );
    }

    private Boolean validLink(String link) {
        try {
            new URI(link).toURL();
            return true;
        } catch (URISyntaxException | MalformedURLException | IllegalArgumentException e) {
            return false;
        }
    }

    private Optional<Creds> splitCreds(String creds) {
        var split = creds.split(":");
        final int two = 2;
        if (split.length != two) {
            return Optional.empty();
        }
        return Optional.of(new Creds(split[0], split[1]));
    }

    private Optional<Creds> parseAuthHeader(String value) {
        final String basicPrefix = "Basic ";
        if (value.length() < basicPrefix.length() || !basicPrefix.equals(value.substring(0, basicPrefix.length()))) {
            return Optional.empty();
        }
        var encoded = value.substring(basicPrefix.length());
        try {
            return splitCreds(new String(Base64.getDecoder().decode(encoded), StandardCharsets.UTF_8));
        } catch (IllegalArgumentException e) {
            return Optional.empty();
        }
    }

    private boolean checkAuth(HttpExchange exc) throws IOException {
        var hdr = exc.getRequestHeaders().get("Authorization").getFirst();
        Creds creds;
        try {
            creds = parseAuthHeader(hdr).orElseThrow();
        } catch (NoSuchElementException e) {
            justCode(422).handle(exc);
            return false;
        }
        boolean authOk;
        try {
            authOk = auth.get(creds.uname).equals(creds.pass);
        } catch (NoSuchElementException e) {
            authOk = false;
        }
        if (!authOk) {
            justCode(401).handle(exc);
        }
        return authOk;
    }
}
