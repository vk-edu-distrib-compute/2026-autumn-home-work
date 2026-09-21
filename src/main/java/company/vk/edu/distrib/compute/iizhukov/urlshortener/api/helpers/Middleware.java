package company.vk.edu.distrib.compute.iizhukov.urlshortener.api.helpers;

@FunctionalInterface
public interface Middleware {
    Response handle(Request request, Handler handler);
}
