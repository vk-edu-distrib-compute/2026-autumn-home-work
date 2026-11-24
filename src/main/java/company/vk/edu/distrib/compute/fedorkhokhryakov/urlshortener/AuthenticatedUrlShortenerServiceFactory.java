package company.vk.edu.distrib.compute.fedorkhokhryakov.urlshortener;

import company.vk.edu.distrib.compute.AbstractHttpServiceFactory;
import company.vk.edu.distrib.compute.urlshortener.UrlShortenerService;
import company.vk.edu.distrib.compute.urlshortener.UrlShortenerAuthTest;

import java.io.IOException;

@UrlShortenerAuthTest
public class AuthenticatedUrlShortenerServiceFactory
    extends AbstractHttpServiceFactory<UrlShortenerService> {

    @Override
    protected UrlShortenerService doCreate(int port) throws IOException {
        return new UrlShortenerServiceImpl(
            port,
            new InMemoryDao<>(),
            new InMemoryUserDao()
        );
    }
}
