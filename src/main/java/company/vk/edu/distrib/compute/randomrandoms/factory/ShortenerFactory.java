package company.vk.edu.distrib.compute.randomrandoms.factory;

import company.vk.edu.distrib.compute.AbstractHttpServiceFactory;
import company.vk.edu.distrib.compute.randomrandoms.urlshortener.Shortener;
import company.vk.edu.distrib.compute.urlshortener.UrlShortenerAuthTest;
import company.vk.edu.distrib.compute.urlshortener.UrlShortenerTest;

import java.io.IOException;

@UrlShortenerTest
@UrlShortenerAuthTest
public class ShortenerFactory extends AbstractHttpServiceFactory<Shortener> {
    @Override
    protected Shortener doCreate(int port) throws IOException {
        return new Shortener(port);
    }
}

