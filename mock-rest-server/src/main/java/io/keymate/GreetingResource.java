package io.keymate;

import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.unchecked.Unchecked;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.resteasy.reactive.RestResponse;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

@Path("/external")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class GreetingResource {

    @ConfigProperty(name = "mock.max-delay-ms", defaultValue = "300")
    long maxDelayMs;

    @ConfigProperty(name = "mock.error-rate", defaultValue = "0.08")
    double errorRate;

    @ConfigProperty(name = "mock.client-error-share", defaultValue = "0.5")
    double clientErrShare;

    /* --- DTOs --- */
    public record OrderRequest(long id, Map<String, Object> data) {
    }

    public record InvoiceRequest(long id, Map<String, Object> data) {
    }

    public record SuccessResponse(String type, long id, String status, String processedAt, long delayMs) {
    }

    @POST
    @Path("/order/process")
    public Uni<RestResponse<SuccessResponse>> processOrder(OrderRequest req) {
        long id = req == null ? -1 : req.id();
        return respond("order", id);
    }

    @POST
    @Path("/invoice/process")
    public Uni<RestResponse<SuccessResponse>> processInvoice(InvoiceRequest req) {
        long id = req == null ? -1 : req.id();
        return respond("invoice", id);
    }

    @SuppressWarnings("java:S2245")
    private Uni<RestResponse<SuccessResponse>> respond(String type, long id) {
        long delay = ThreadLocalRandom.current().nextLong(0, Math.max(1, maxDelayMs + 1));
        boolean shouldError = ThreadLocalRandom.current().nextDouble() < errorRate;

        return Uni.createFrom().voidItem()
                .onItem().delayIt().by(Duration.ofMillis(delay))
                .onItem().transform(Unchecked.function(v -> {
                    if (shouldError) {
                        boolean clientErr = ThreadLocalRandom.current().nextDouble() < clientErrShare;
                        int status = clientErr ? 400 : 500;
                        // Mapper'a fırlat
                        throw new MockFailure(status,
                                clientErr ? "Bad Request" : "Internal Server Error",
                                "Mocked failure for testing",
                                delay);
                    }
                    var body = new SuccessResponse(
                            type, id, "PROCESSED", OffsetDateTime.now().toString(), delay);
                    return RestResponse.ResponseBuilder.create(RestResponse.Status.CREATED, body)
                            .header("X-Mock-Delay", Long.toString(delay))
                            .build();
                }));
    }
}
