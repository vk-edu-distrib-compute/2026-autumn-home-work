package company.vk.edu.distrib.compute.mrglaster.urlshortener.controller.manager;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.mrglaster.urlshortener.annotation.Route;
import company.vk.edu.distrib.compute.mrglaster.urlshortener.controller.enums.StatusCode;
import company.vk.edu.distrib.compute.mrglaster.urlshortener.controller.network.NetworkInteractable;
import company.vk.edu.distrib.compute.mrglaster.urlshortener.service.AuthorizationService;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class ControllerManager implements NetworkInteractable {

    private final List<RouteDefinition> routes = new ArrayList<>();
    private final AuthorizationService authService;
    private static final String ROOT_PATH = "/";

    public ControllerManager(AuthorizationService authService) {
        this.authService = authService;
    }

    private static String[] splitPath(String urlPath) {
        String processableUrlPath = urlPath;
        if (processableUrlPath == null || processableUrlPath.isEmpty() || ROOT_PATH.equals(processableUrlPath)) {
            return new String[0];
        }
        if (processableUrlPath.startsWith(ROOT_PATH)) {
            processableUrlPath = processableUrlPath.substring(1);
        }
        if (processableUrlPath.endsWith(ROOT_PATH)) {
            processableUrlPath = processableUrlPath.substring(0, processableUrlPath.length() - 1);
        }
        if (processableUrlPath.isEmpty()) {
            return new String[0];
        }
        return processableUrlPath.split(ROOT_PATH);
    }

    public void addController(Object controller) {
        Class<?> clazz = controller.getClass();
        for (Method method : clazz.getDeclaredMethods()) {
            if (method.isAnnotationPresent(Route.class)) {
                Route route = method.getAnnotation(Route.class);
                routes.add(new RouteDefinition(route.method().toUpperCase(Locale.US),
                        route.path(),
                        route.requiresAuthorization(),
                        method, controller));
            }
        }
    }

    public void register(HttpServer server) {
        server.createContext(ROOT_PATH, exchange -> {
            String requestPath = exchange.getRequestURI().getPath();
            String requestMethod = exchange.getRequestMethod().toUpperCase(Locale.US);

            RouteMatch match = findMatchingRoute(requestMethod, requestPath);

            if (match != null) {
                if (match.route.requiresAuthorization) {
                    try {
                        String authHeader = exchange.getRequestHeaders().getFirst("Authorization");
                        if (!authService.checkBasicAuth(authHeader)) {
                            sendStatusCodeResponse(exchange, StatusCode.HTTP_UNAUTHORIZED);
                            return;
                        }
                    } catch (Exception e) {
                        sendStatusCodeResponse(exchange, StatusCode.HTTP_UNAUTHORIZED);
                        return;
                    }
                }
                invokeHandler(match, exchange);
            } else {
                exchange.sendResponseHeaders(404, -1);
                exchange.close();
            }
        });
    }

    private RouteMatch findMatchingRoute(String method, String path) {
        String[] pathSegments = splitPath(path);
        for (RouteDefinition route : routes) {
            if (!route.httpMethod.equals(method) || route.pathSegments.length != pathSegments.length) {
                continue;
            }
            Optional<Map<String, String>> extractedParams = matchRoute(route, pathSegments);
            if (extractedParams.isPresent()) {
                return new RouteMatch(route, extractedParams.get());
            }
        }
        return null;
    }

    private Optional<Map<String, String>> matchRoute(RouteDefinition route, String... pathSegments) {
        Map<String, String> extractedParams = new ConcurrentHashMap<>();
        for (int i = 0; i < route.pathSegments.length; i++) {
            String routeSegment = route.pathSegments[i];
            String pathSegment = pathSegments[i];

            if (isPathParam(routeSegment)) {
                extractedParams.put(extractParamName(routeSegment), pathSegment);
            } else if (!routeSegment.equals(pathSegment)) {
                return Optional.empty();
            }
        }
        return Optional.of(extractedParams);
    }

    private static boolean isPathParam(String segment) {
        return segment.startsWith("{") && segment.endsWith("}");
    }

    private static String extractParamName(String segment) {
        return segment.substring(1, segment.length() - 1);
    }

    private void invokeHandler(RouteMatch match, HttpExchange exchange) throws IOException {
        Method method = match.route.method;
        Object instance = match.route.instance;

        try (HttpExchange ex = exchange) {
            Object[] args = new Object[method.getParameterCount()];
            for (int i = 0; i < method.getParameterCount(); i++) {
                Class<?> paramType = method.getParameterTypes()[i];
                if (paramType == HttpExchange.class) {
                    args[i] = ex;
                } else if (paramType == Map.class) {
                    args[i] = match.pathParams;
                } else {
                    sendStatusCodeResponse(ex, StatusCode.HTTP_INTERNAL_SERVER_ERROR);
                    return;
                }
            }

            try {
                method.invoke(instance, args);
            } catch (Exception e) {
                sendStatusCodeResponse(ex, StatusCode.HTTP_INTERNAL_SERVER_ERROR);
                return;
            }

            if (ex.getResponseCode() == -1) {
                sendStatusCodeResponse(ex, StatusCode.HTTP_OK);
            }
        }
    }

    private static class RouteDefinition {
        final String httpMethod;
        final String[] pathSegments;
        final boolean requiresAuthorization;
        final Method method;
        final Object instance;

        RouteDefinition(String httpMethod, String path, boolean requiresAuthorization, Method method, Object instance) {
            this.httpMethod = httpMethod;
            this.requiresAuthorization = requiresAuthorization;
            this.method = method;
            this.instance = instance;
            this.pathSegments = splitPath(path);
        }
    }

    private record RouteMatch(RouteDefinition route, Map<String, String> pathParams) {
    }
}
