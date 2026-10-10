package company.vk.edu.distrib.compute.randomrandoms.util;

import com.sun.net.httpserver.HttpHandler;

public final class HandlerConstants {
    private HandlerConstants() {
    }

    public static HttpHandler justCode(int code) {
        return exc -> {
            exc.sendResponseHeaders(code, 0);
            exc.close();
        };
    }

    public static HttpHandler codeAndText(int code, byte[] text) {
        return exc -> {
            exc.getResponseHeaders().add("Content-Type", "text/html; charset=utf-8");
            exc.sendResponseHeaders(code, text.length);
            exc.getResponseBody().write(text);
            exc.getResponseBody().close();
            exc.close();
        };
    }
}
