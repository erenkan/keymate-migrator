package io.keymate;

import io.smallrye.mutiny.Uni;
import io.keymate.api.InvoiceApiClient;
import io.keymate.api.OrderApiClient;
import io.keymate.model.InvoiceJob;
import io.keymate.model.Job;
import io.keymate.model.OrderJob;
import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.rest.client.inject.RestClient;

/**
 * Routes a claimed {@link Job} to the appropriate handler based on domain/table/type.
 *
 * <p>Each handler is responsible for transforming the job payload into a target-side request
 * (e.g., Keycloak ingestion endpoint) and handling success/failure semantics.</p>
 */
@ApplicationScoped
public class JobRouter {

    final OrderApiClient orderApi;
    final InvoiceApiClient invoiceApi;

    public JobRouter(@RestClient OrderApiClient orderApi,
                     @RestClient InvoiceApiClient invoiceApi) {
        this.orderApi = orderApi;
        this.invoiceApi = invoiceApi;
    }

    public Uni<Void> process(Job job) {
        return switch (job.table()) {
            case "orders" -> processOrder((OrderJob) job);
            case "invoices" -> processInvoice((InvoiceJob) job);
            default -> Uni.createFrom().voidItem();
        };
    }

    private Uni<Void> processOrder(OrderJob job) {
        // simple transformation example
        String transformed = job.payload().toUpperCase();
        return orderApi.process(new OrderApiClient.Req(job.pk(), transformed)).replaceWithVoid();
    }

    private Uni<Void> processInvoice(InvoiceJob job) {
        String transformed = job.payload().toLowerCase();
        return invoiceApi.process(new InvoiceApiClient.Req(job.pk(), transformed)).replaceWithVoid();
    }
}
