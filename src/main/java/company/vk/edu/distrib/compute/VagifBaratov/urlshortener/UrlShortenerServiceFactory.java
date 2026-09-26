package company.vk.edu.distrib.compute.vagifbaratov.urlshortener;

import company.vk.edu.distrib.compute.AbstractHttpServiceFactory;

import java.io.IOException;

public class UrlShortenerServiceFactory extends AbstractHttpServiceFactory<UrlShortenerServiceImpl> {
    @Override
    protected UrlShortenerServiceImpl doCreate(int port) throws IOException {
        return new UrlShortenerServiceImpl(port);
    }
}
