package io.keymate;

public class MockFailure extends RuntimeException {
    private final int status;
    private final String title;
    private final String detail;
    private final long delayMs;

    public MockFailure(int status, String title, String detail, long delayMs) {
        super(title + ": " + detail);
        this.status = status;
        this.title = title;
        this.detail = detail;
        this.delayMs = delayMs;
    }

    public int getStatus() {
        return status;
    }

    public String getTitle() {
        return title;
    }

    public String getDetail() {
        return detail;
    }

    public long getDelayMs() {
        return delayMs;
    }
}

