package company.vk.edu.distrib.compute.akravchenya.urlshortener;

import com.sun.net.httpserver.HttpExchange;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.regex.Pattern;

/**
 * Вспомогательные средства проверки и извлечения данных.
 */
final class Urls {

    private static final Pattern ID_PATTERN = Pattern.compile("[A-Za-z0-9]{10}");
    private static final Pattern LONG_LINK_PATTERN = Pattern.compile("^(?i:https?)://[^\\s/$.?#][^\\s]*$");

    private Urls() {
    }

    /**
     * Проверить идентификатор ссылки и возвращает его без изменений.
     *
     * @param id идентификатор для проверки
     * @return исходный идентификатор
     * @throws IllegalArgumentException если идентификатор не состоит из 10 буквенно-цифровых символов
     */
    static String requireId(String id) {
        if (id == null || !ID_PATTERN.matcher(id).matches()) {
            throw new IllegalArgumentException("link id must be 10 alpha-numeric characters");
        }
        return id;
    }

    /**
     * Проверить длинную ссылку, указанную в теле запроса.
     *
     * @param longLink ссылка для проверки
     * @throws IllegalArgumentException если ссылка не является допустимым URL-адресом HTTP(S)
     */
    static void requireLongLink(String longLink) {
        if (longLink == null || !LONG_LINK_PATTERN.matcher(longLink).matches()) {
            throw new IllegalArgumentException("long link is not a valid URL");
        }
    }

    /**
     * Прочитать все тело запроса в виде строки в кодировке UTF-8.
     *
     * @param exchange обмен данными, тело которого необходимо считать
     * @return текст тела запроса
     * @throws IOException если тело не удалось прочитать
     */
    static String readBody(HttpExchange exchange) throws IOException {
        return new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
    }

    /**
     * Извлечь часть id из пути запроса, зарегистрированного с префиксом контекста.
     *
     * @param exchange проверяемый обмен данными
     * @return id после префикса контекста или пустая строка, если путь совпадает с префиксом
     * @throws IllegalArgumentException если путь не соответствует шаблону префикса контекста
     */
    static String idAfterContext(HttpExchange exchange) {
        var contextPath = exchange.getHttpContext().getPath();
        var requestPath = exchange.getRequestURI().getPath();
        if (requestPath.equals(contextPath)) {
            return "";
        }
        if (!requestPath.startsWith(contextPath + "/")) {
            throw new IllegalArgumentException("unsupported path: " + requestPath);
        }
        return requestPath.substring(contextPath.length() + 1);
    }
}
