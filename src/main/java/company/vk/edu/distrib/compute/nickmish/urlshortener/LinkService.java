package company.vk.edu.distrib.compute.nickmish.urlshortener;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.nickmish.urlshortener.utils.IdGeneratorUtils;
import company.vk.edu.distrib.compute.nickmish.urlshortener.utils.IdValidatorUtils;
import company.vk.edu.distrib.compute.nickmish.urlshortener.utils.LinkValidatorUtils;

import java.io.IOException;
import java.util.NoSuchElementException;

public final class LinkService {
    private final Dao<String> links;

    public LinkService(Dao<String> links) {
        this.links = links;
    }

    public String get(String id) throws IOException {
        IdValidatorUtils.validate(id);
        return links.get(id);
    }

    public String create(String longLink) throws IOException {
        LinkValidatorUtils.validate(longLink);
        String id;
        do {
            id = IdGeneratorUtils.generate();
        } while (exists(id));
        links.upsert(id, longLink);
        return id;
    }

    public void update(String id, String longLink) throws IOException {
        IdValidatorUtils.validate(id);
        LinkValidatorUtils.validate(longLink);
        links.get(id);
        links.upsert(id, longLink);
    }

    public void delete(String id) throws IOException {
        IdValidatorUtils.validate(id);
        links.delete(id);
    }

    private boolean exists(String id) throws IOException {
        try {
            links.get(id);
            return true;
        } catch (NoSuchElementException e) {
            return false;
        }
    }
}
