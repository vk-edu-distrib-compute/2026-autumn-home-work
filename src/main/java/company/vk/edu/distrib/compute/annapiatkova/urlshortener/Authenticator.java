package company.vk.edu.distrib.compute.annapiatkova.urlshortener;

import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.util.NoSuchElementException;

class Authenticator {
    private final Dao<String> users;

    Authenticator() {
        users = new DaoImpl();
    }

    void addUser(String username, String password) throws IOException {
        users.upsert(username, password);
    }

    boolean checkCredentials(String username, String password) throws IOException {
        try {
            String expectedPassword = users.get(username);
            return expectedPassword.equals(password);
        } catch (NoSuchElementException e) {
            return false;
        }
    }
}
