package company.vk.edu.distrib.compute.iizhukov.urlshortener;

import java.io.IOException;

import company.vk.edu.distrib.compute.AbstractHttpServiceFactory;
import org.jspecify.annotations.NonNull;

public class ApplicationFactory extends AbstractHttpServiceFactory<@NonNull Application> {
    @Override
    protected Application doCreate(int port) throws IOException {
        var app = new Application();
        app.init(port);
        return app;
    }
}
