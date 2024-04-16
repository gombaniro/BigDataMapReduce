package org.bigdata.common;

public class LogEntry {
    private String ipAddress;
    private String dateTime;
    private String method;
    private String uri;
    private String protocol;
    private int status;
    private long bytesSent;

    private LogEntry(Builder builder) {
        this.ipAddress = builder.ipAddress;
        this.dateTime = builder.dateTime;
        this.method = builder.method;
        this.uri = builder.uri;
        this.protocol = builder.protocol;
        this.status = builder.status;
        this.bytesSent = builder.bytesSent;
    }

    // Getters for fields (omitted for brevity)

    public static class Builder {
        private String ipAddress;
        private String dateTime;
        private String method;
        private String uri;
        private String protocol;
        private int status;
        private long bytesSent;

        public Builder ipAddress(String ipAddress) {
            this.ipAddress = ipAddress;
            return this;
        }

        public Builder dateTime(String dateTime) {
            this.dateTime = dateTime;
            return this;
        }

        public Builder method(String method) {
            this.method = method;
            return this;
        }

        public Builder uri(String uri) {
            this.uri = uri;
            return this;
        }

        public Builder protocol(String protocol) {
            this.protocol = protocol;
            return this;
        }

        public Builder status(int status) {
            this.status = status;
            return this;
        }

        public Builder bytesSent(long bytesSent) {
            this.bytesSent = bytesSent;
            return this;
        }

        public LogEntry build() {
            return new LogEntry(this);
        }
    }

    public String getIpAddress() {
        return ipAddress;
    }

    public long getBytesSent() {
        return bytesSent;
    }
}

