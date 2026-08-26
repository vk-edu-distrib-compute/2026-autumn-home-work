package company.vk.edu.distrib.compute;

import java.util.concurrent.CompletableFuture;

public interface HttpService {
    /**
     * Bind storage to HTTP port and start listening.
     *
     * <p>
     * May be called only once.
     */
    void start();

    /**
     * Stop listening and free all the resources.
     *
     * <p>
     * May be called only once and after {@link #start()}.
     */
    void stop();

    /**
     * Optional helper method to await service termination.
     *
     * <p>
     * May be called only once and after {@link #start()}.
     */
    default CompletableFuture<Void> awaitTermination() {
        return CompletableFuture.completedFuture(null);
    }
}
