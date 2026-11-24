package company.vk.edu.distrib.compute.nosorozhek.urlshortener;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.moderation.LinkCreatedEvent;
import company.vk.edu.distrib.compute.nosorozhek.moderation.LinkCreatedPublisher;
import company.vk.edu.distrib.compute.nosorozhek.urlshortener.validation.LinkIdValidationHelper;
import company.vk.edu.distrib.compute.nosorozhek.urlshortener.validation.LinkValidationHelper;

import java.io.IOException;
import java.security.SecureRandom;
import java.time.Instant;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;

public class UrlShortener {
    private Dao<String> urlDao;
    private Optional<LinkCreatedPublisher> publisher = Optional.empty();

    public UrlShortener(Dao<String> urlDao) {
        this.urlDao = urlDao;
    }

    public String create(String link) throws IOException {
        LinkValidationHelper.validate(link);

        String id = LinkIdGenerationHelper.generateRandomId();
        urlDao.upsert(id, link);
        publisher.ifPresent(linkCreatedPublisher ->
                linkCreatedPublisher.publish(new LinkCreatedEvent(id, link, Instant.now())));
        return id;
    }

    public String get(String id) throws IOException {
        LinkIdValidationHelper.validate(id);
        return urlDao.get(id);
    }

    public void update(String id, String link) throws NoSuchElementException, IOException {
        LinkIdValidationHelper.validate(id);
        LinkValidationHelper.validate(link);

        urlDao.get(id); // throw in case the id doesnt persist in the dao
        urlDao.upsert(id, link);
    }

    public void delete(String id) throws IOException {
        LinkIdValidationHelper.validate(id);
        urlDao.delete(id);
    }

    public void setLinkCreatedPublisher(LinkCreatedPublisher publisher) {
        this.publisher = Optional.of(publisher);
    }

    void setLinksDao(Dao<String> dao) {
        this.urlDao = Objects.requireNonNull(dao);
    }

    private static final class LinkIdGenerationHelper {
        private static final String ALPHANUMERIC = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        private static final SecureRandom RANDOM = new SecureRandom();
        private static final int ID_LENGTH = 10;

        public static String generateRandomId() {
            StringBuilder sb = new StringBuilder(ID_LENGTH);
            for (int i = 0; i < ID_LENGTH; i++) {
                int randomIndex = RANDOM.nextInt(ALPHANUMERIC.length());
                sb.append(ALPHANUMERIC.charAt(randomIndex));
            }
            return sb.toString();
        }
    }
}
