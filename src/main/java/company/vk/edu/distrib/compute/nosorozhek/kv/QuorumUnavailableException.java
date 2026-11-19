package company.vk.edu.distrib.compute.nosorozhek.kv;

import java.io.IOException;

public class QuorumUnavailableException extends IOException {
    private static final long serialVersionUID = 1L;

    public QuorumUnavailableException() {
        super("Not enough nodes available");
    }
}
