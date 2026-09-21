package company.vk.edu.distrib.compute.iizhukov.urlshortener.db.dao;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.iizhukov.urlshortener.db.DataValidationUtils;
import company.vk.edu.distrib.compute.iizhukov.urlshortener.db.FileStorage;
import company.vk.edu.distrib.compute.iizhukov.urlshortener.db.models.LinkModel;
import org.jspecify.annotations.NonNull;

public final class LinksDao implements Dao<@NonNull LinkModel> {
    private static LinksDao instance;
    private final Map<String, LinkModel> links;
    private final FileStorage<LinkModel> storage;

    private LinksDao() throws IOException {
        storage = new FileStorage<>(
                new File("database/links.csv"),
                LinkModel.class
        );
        links = storage.read();
    }

    public static synchronized LinksDao create() {
        if (instance == null) {
            try {
                instance = new LinksDao();
            } catch (IOException e) {
                throw new RuntimeException("cant open file", e);
            }
        }

        return instance;
    }

    @Override
    public LinkModel get(String key) throws IllegalArgumentException {
        DataValidationUtils.validateKey(key);
        return links.get(key);
    }

    @Override
    public void upsert(String key, LinkModel value) throws IllegalArgumentException {
        DataValidationUtils.validateKey(key);
        DataValidationUtils.validateUrl(value.url());
        links.put(key, value);

        try {
            storage.write(links);
        } catch (IOException e) {
            throw new RuntimeException("cant write file =(", e);
        }
    }

    @Override
    public void delete(String key) throws IllegalArgumentException {
        DataValidationUtils.validateKey(key);
        links.remove(key);

        try {
            storage.write(links);
        } catch (IOException e) {
            throw new RuntimeException("cant write file =(", e);
        }
    }

    @Override
    public void close() {
        try {
            storage.write(links);
            storage.close();
        } catch (IOException e) {
            throw new RuntimeException("cant close file", e);
        }
    }
}
