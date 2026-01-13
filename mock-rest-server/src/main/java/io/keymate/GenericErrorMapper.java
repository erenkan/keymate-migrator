package io.keymate;

import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;

import java.util.Map;

@Provider
public class GenericErrorMapper implements ExceptionMapper<Throwable> {
    public Response toResponse(Throwable ex) {
        var body = Map.of("error", "Internal Server Error", "detail", ex.getMessage());
        return Response.status(500).type(MediaType.APPLICATION_JSON).entity(body).build();
    }
}
