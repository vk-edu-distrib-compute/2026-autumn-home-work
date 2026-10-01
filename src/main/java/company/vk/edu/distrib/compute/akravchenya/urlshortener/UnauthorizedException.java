package company.vk.edu.distrib.compute.akravchenya.urlshortener;

import java.io.Serial;

/**
 * Неудачная проверка базовой аутентификации {@code 401 Unauthorized}.
 */
class UnauthorizedException extends RuntimeException {

    @Serial
    private static final long serialVersionUID = 1L;

    UnauthorizedException(String message, Throwable cause) {
        super(message, cause);
    }

    UnauthorizedException(String message) {
        super(message);
    }
}
