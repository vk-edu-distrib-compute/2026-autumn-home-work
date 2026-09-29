package company.vk.edu.distrib.compute.kl1dd.urlshortener;

import com.sun.net.httpserver.HttpExchange;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.NoSuchElementException;

public class AuthService {
    private final MyDao authDao = new MyDao();

    public void createUser(String username, String password) throws IOException {
        authDao.upsert(username, password);
    }

    public boolean isAuthed(HttpExchange exchange) throws IOException {
        List<String> authHeaders = exchange.getRequestHeaders().get("Authorization");
        if (authHeaders == null || authHeaders.isEmpty()) {
            return false;
        }
        String auth = authHeaders.getFirst();
        if (auth == null || !auth.startsWith("Basic ")) {
            return false;
        }

        String nonBasicStr;
        try {
            String basicStr = auth.substring("Basic ".length());
            nonBasicStr = new String(Base64.getDecoder().decode(basicStr), StandardCharsets.UTF_8);
        } catch (IllegalArgumentException e) {
            return false;
        }
        String[] splitStr = nonBasicStr.split(":", 2);
        if (splitStr.length != 2) {
            return false;
        }

        String username = splitStr[0];
        String password = splitStr[1];
        try {
            String realPassword = authDao.get(username);
            return realPassword.equals(password);
        } catch (NoSuchElementException e) {
            return false;
        }
    }
}
