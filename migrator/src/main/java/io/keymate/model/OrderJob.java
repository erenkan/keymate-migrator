package io.keymate.model;

public record OrderJob(long pk, String payload, int attempt) implements Job {

    @Override
    public String table() {
        return "orders";
    }
}
