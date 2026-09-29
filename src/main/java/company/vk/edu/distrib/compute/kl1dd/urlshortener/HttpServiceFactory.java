package company.vk.edu.distrib.compute.kl1dd.urlshortener;

import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.AbstractHttpServiceFactory;
import company.vk.edu.distrib.compute.HttpService;
import company.vk.edu.distrib.compute.urlshortener.UrlShortenerService;
import company.vk.edu.distrib.compute.urlshortener.UrlShortenerTest;

import java.io.IOException;
import java.net.InetSocketAddress;

@UrlShortenerTest
public class HttpServiceFactory extends AbstractHttpServiceFactory<UrlShortenerService> {
    @Override
    protected UrlShortenerService doCreate(int port) throws IOException {
        HttpServer httpService = HttpServer.create(new InetSocketAddress(port), 0);
        MyUrlShortenerService myUrlShortenerService = new MyUrlShortenerService(httpService);
        return myUrlShortenerService;
    }

    public HttpServiceFactory() {
    }
}
