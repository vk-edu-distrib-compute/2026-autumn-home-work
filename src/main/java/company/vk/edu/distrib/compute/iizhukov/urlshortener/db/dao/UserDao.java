package company.vk.edu.distrib.compute.iizhukov.urlshortener.db.dao;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.iizhukov.urlshortener.db.FileStorage;
import company.vk.edu.distrib.compute.iizhukov.urlshortener.db.models.UserModel;
import org.jspecify.annotations.NonNull;

public final class UserDao implements Dao<@NonNull UserModel> {
    private static UserDao instance;
    private final Map<String, UserModel> users;
    private final FileStorage<UserModel> storage;

    private UserDao() throws IOException {
        storage = new FileStorage<>(
                new File("database/users.csv"),
                UserModel.class
        );
        users = storage.read();
    }

    public static synchronized UserDao create() {
        if (instance == null) {
            try {
                instance = new UserDao();
            } catch (IOException e) {
                throw new RuntimeException("cant open file", e);
            }
        }

        return instance;
    }

    @Override
    public UserModel get(String key) throws IllegalArgumentException {
        return users.get(key);
    }

    @Override
    public void upsert(String key, UserModel value) throws IllegalArgumentException {
        users.put(key, value);

        try {
            storage.write(users);
        } catch (IOException e) {
            throw new RuntimeException("cant write file =(", e);
        }
    }

    @Override
    public void delete(String key) throws IllegalArgumentException {
        users.remove(key);

        try {
            storage.write(users);
        } catch (IOException e) {
            throw new RuntimeException("cant write file =(", e);
        }
    }

    @Override
    public void close() {
        try {
            storage.write(users);
            storage.close();
        } catch (IOException e) {
            throw new RuntimeException("cant close file", e);
        }
    }
}
