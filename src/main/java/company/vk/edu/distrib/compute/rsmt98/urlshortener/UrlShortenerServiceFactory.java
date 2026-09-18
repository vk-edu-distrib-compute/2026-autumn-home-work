package company.vk.edu.distrib.compute.rsmt98.urlshortener;

import company.vk.edu.distrib.compute.AbstractHttpServiceFactory;
import company.vk.edu.distrib.compute.urlshortener.UrlShortenerService;

public final class UrlShortenerServiceFactory
        extends AbstractHttpServiceFactory<UrlShortenerService> {
    @Override
    protected UrlShortenerService doCreate(int port) {
        return new PersistentUrlShortenerService(port);
    }
}
