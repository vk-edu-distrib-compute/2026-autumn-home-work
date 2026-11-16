package company.vk.edu.distrib.compute.akravchenya.kv;

final class QuorumNotReachedException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    QuorumNotReachedException(String message) {
        super(message);
    }
}
