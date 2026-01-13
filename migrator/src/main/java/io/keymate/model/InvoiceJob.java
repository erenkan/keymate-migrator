package io.keymate.model;

public record InvoiceJob(long pk, String payload, int attempt) implements Job {

    @Override
    public String table() {
        return "invoices";
    }
}
