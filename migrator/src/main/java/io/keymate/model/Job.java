package io.keymate.model;

public sealed interface Job permits OrderJob, InvoiceJob {

    String table();

    long pk();

    String payload();

    int attempt();
}
