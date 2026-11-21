package company.vk.edu.distrib.compute.iizhukov.shared.http;

@FunctionalInterface
public interface Middleware {
    Handler apply(Handler handler);
}
