package company.vk.edu.distrib.compute.akravchenya.urlshortener;

import company.vk.edu.distrib.compute.AbstractHttpServiceFactory;
import company.vk.edu.distrib.compute.urlshortener.UrlShortenerAuthTest;
import company.vk.edu.distrib.compute.urlshortener.UrlShortenerTest;

import java.io.IOException;

/**
 * Создает персонализированный {@link UrlShortenerServiceImpl}.
 */
@UrlShortenerTest
@UrlShortenerAuthTest
public class UrlShortenerServiceFactory extends AbstractHttpServiceFactory<UrlShortenerServiceImpl> {

    @Override
    protected UrlShortenerServiceImpl doCreate(int port) throws IOException {
        return new UrlShortenerServiceImpl(port);
    }
}
