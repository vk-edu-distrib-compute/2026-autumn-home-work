package company.vk.edu.distrib.compute.nickmish.urlshortener;

import company.vk.edu.distrib.compute.AbstractHttpServiceFactory;
import company.vk.edu.distrib.compute.urlshortener.UrlShortenerAuthTest;
import company.vk.edu.distrib.compute.urlshortener.UrlShortenerService;
import company.vk.edu.distrib.compute.urlshortener.UrlShortenerTest;

@UrlShortenerTest
@UrlShortenerAuthTest
public final class UrlShortenerServiceFactory extends AbstractHttpServiceFactory<UrlShortenerService> {
    @Override
    protected UrlShortenerService doCreate(int port) {
        return new UrlShortener(port);
    }
}
