package company.vk.edu.distrib.compute;

import java.io.IOException;

public abstract class AbstractHttpServiceFactory<T> {
    private static final long MAX_HEAP = 128 * 1024 * 1024;

    /**
     * Construct a service instance.
     *
     * @param port port to bind HTTP server to
     * @return a storage instance
     */
    public T create(int port) throws IOException {
        if (Runtime.getRuntime().maxMemory() > MAX_HEAP) {
            throw new IllegalStateException("The heap is too big. Consider setting Xmx.");
        }

        if (port <= 0 || 65536 <= port) {
            throw new IllegalArgumentException("Port out of range");
        }

        return doCreate(port);
    }

    protected abstract T doCreate(int port) throws IOException;
}
