package company.vk.edu.distrib.compute.fedorkhokhryakov.urlshortener;

import company.vk.edu.distrib.compute.AbstractHttpServiceFactory;
import company.vk.edu.distrib.compute.urlshortener.UrlShortenerService;
import company.vk.edu.distrib.compute.urlshortener.UrlShortenerTest;

import java.io.IOException;

@UrlShortenerTest
public class UrlShortenerServiceFactory
    extends AbstractHttpServiceFactory<UrlShortenerService> {

    @Override
    protected UrlShortenerService doCreate(int port) throws IOException {
        return new UrlShortenerServiceImpl(port, new InMemoryDao<>());
    }
}
