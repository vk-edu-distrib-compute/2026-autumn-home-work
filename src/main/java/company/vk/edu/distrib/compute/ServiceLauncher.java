package company.vk.edu.distrib.compute;

import module java.base;

import company.vk.edu.distrib.compute.urlshortener.DummyUrlShortenerServiceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServiceLauncher {

    private static final int DEFAULT_PORT = 8080;
    private static final Logger log = LoggerFactory.getLogger(ServiceLauncher.class);
    
    void main(String... args) throws IOException, ClassNotFoundException, NoSuchMethodException,
        InvocationTargetException, InstantiationException, IllegalAccessException {

        var port = getPort();
        var service = getServiceFactory().create(port);
        Runtime.getRuntime().addShutdownHook(new Thread(service::stop));
        log.info("Starting '{}' service on port {}", service.getClass().getName(), port);
        service.start();
    }

    @SuppressWarnings("unchecked")
    private static AbstractHttpServiceFactory<? extends HttpService> getServiceFactory() throws ClassNotFoundException,
        NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {

        var serviceFactoryClass = System.getenv("SERVICE_FACTORY");
        if (serviceFactoryClass == null || serviceFactoryClass.isBlank()) {
            serviceFactoryClass = DummyUrlShortenerServiceFactory.class.getName();
        }
        return (AbstractHttpServiceFactory<? extends HttpService>) Class.forName(serviceFactoryClass)
            .getDeclaredConstructor().newInstance();
    }

    private static int getPort() {
        var envPort = System.getenv("SERVICE_PORT");
        if (envPort == null || envPort.isBlank()) {
            return DEFAULT_PORT;
        }
        try {
            return Integer.parseInt(envPort);
        } catch (NumberFormatException e) {
            return DEFAULT_PORT;
        }
    }
}
