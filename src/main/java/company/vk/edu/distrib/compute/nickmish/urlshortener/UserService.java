package company.vk.edu.distrib.compute.nickmish.urlshortener;

import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;

public final class UserService {
    private final Dao<String> users;

    public UserService(Dao<String> users) {
        this.users = users;
    }

    public void register(String credentials) throws IOException {
        int sep = credentials.indexOf(':');
        if (sep < 0) {
            throw new IllegalArgumentException("Credentials must be in format 'username:password'");
        }
        String username = credentials.substring(0, sep);
        String password = credentials.substring(sep + 1);
        users.upsert(username, password);
    }
}
