package company.vk.edu.distrib.compute.robert.kv.api.v0;

import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;

import com.sun.net.httpserver.HttpExchange;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.robert.api.models.BadRequestException;
import company.vk.edu.distrib.compute.robert.api.models.HttpStatus;
import company.vk.edu.distrib.compute.robert.api.models.Request;
import company.vk.edu.distrib.compute.robert.api.models.Response;
import company.vk.edu.distrib.compute.robert.kv.RobertReplicationCoordinator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReplicatedEntityHandler extends EntityHandler {
    private static final Logger log = LoggerFactory.getLogger(ReplicatedEntityHandler.class);
    private static final String ACK_PARAMETER = "ack";

    private final RobertReplicationCoordinator coordinator;

    public ReplicatedEntityHandler(Dao<byte[]> inputDao, RobertReplicationCoordinator inputCoordinator) {
        super(inputDao);
        coordinator = inputCoordinator;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        String replica = exchange.getRequestHeaders().getFirst(RobertReplicationCoordinator.REPLICA_HEADER);
        String method = exchange.getRequestMethod();
        if ("1".equals(replica) || !("GET".equals(method) || "PUT".equals(method) || "DELETE".equals(method))) {
            super.handle(exchange);
            return;
        }

        CompletableFuture<Response> result;
        try {
            Request request = Request.from(exchange);
            result = coordinator.replicate(request, extractId(request), extractParameter(request, ACK_PARAMETER));
        } catch (BadRequestException e) {
            result = completedResponse(HttpStatus.BAD_REQUEST);
        } catch (NoSuchElementException e) {
            result = completedResponse(HttpStatus.NOT_FOUND);
        } catch (IllegalArgumentException e) {
            result = completedResponse(HttpStatus.UNPROCESSABLE_CONTENT);
        } catch (IOException e) {
            result = completedResponse(HttpStatus.SERVICE_UNAVAILABLE);
        }

        result.thenAccept(response -> {
            try {
                sendResponse(exchange, response);
            } catch (IOException e) {
                exchange.close();
                log.atDebug().setCause(e).log("Could not send replicated response");
            }
        });
    }

    private static CompletableFuture<Response> completedResponse(HttpStatus status) {
        return CompletableFuture.completedFuture(Response.builder()
            .setStatus(status.code())
            .build());
    }
}
