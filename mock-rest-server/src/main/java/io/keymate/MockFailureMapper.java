package io.keymate;

import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;

import java.time.OffsetDateTime;
import java.util.Map;

@Provider
public class MockFailureMapper implements ExceptionMapper<MockFailure> {

    @Override
    public Response toResponse(MockFailure e) {
        var body = Map.of(
                "error", e.getTitle(),
                "detail", e.getDetail(),
                "delayMs", e.getDelayMs(),
                "at", OffsetDateTime.now().toString()
        );
        return Response.status(e.getStatus())
                .header("X-Mock-Delay", e.getDelayMs())
                .type(MediaType.APPLICATION_JSON)
                .entity(body)
                .build();
    }
}
