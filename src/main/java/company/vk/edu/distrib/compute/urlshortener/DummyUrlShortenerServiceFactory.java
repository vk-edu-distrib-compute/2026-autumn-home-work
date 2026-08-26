package company.vk.edu.distrib.compute.urlshortener;

import java.io.IOException;

import company.vk.edu.distrib.compute.AbstractHttpServiceFactory;

public class DummyUrlShortenerServiceFactory extends AbstractHttpServiceFactory<UrlShortenerService> {
    @Override
    protected UrlShortenerService doCreate(int port) throws IOException {
        return new DummyUrlShortenerService();
    }
}
