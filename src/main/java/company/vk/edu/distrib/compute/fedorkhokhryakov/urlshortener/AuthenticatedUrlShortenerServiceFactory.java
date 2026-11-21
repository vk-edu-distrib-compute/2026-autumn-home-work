package company.vk.edu.distrib.compute.fedorkhokhryakov.urlshortener;

import company.vk.edu.distrib.compute.AbstractHttpServiceFactory;
import company.vk.edu.distrib.compute.urlshortener.UrlShortenerService;

import java.io.IOException;

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
