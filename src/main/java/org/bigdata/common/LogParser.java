package org.bigdata.common;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LogParser {
    private static final String LOG_ENTRY_PATTERN =
            "^([\\d.]+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(\\S+) (\\S+) (\\S+)\" (\\d{3}) (\\d+)";

    private static final Pattern PATTERN = Pattern.compile(LOG_ENTRY_PATTERN);

    public static LogEntry parseLogEntry(String logEntryLine) {
        Matcher matcher = PATTERN.matcher(logEntryLine);
        if (!matcher.matches()) {
            System.err.println("Invalid log entry: " + logEntryLine);
            return null;
        }

        String ipAddress = matcher.group(1);
        String dateTime = matcher.group(4);
        String method = matcher.group(5);
        String uri = matcher.group(6);
        String protocol = matcher.group(7);
        int status = Integer.parseInt(matcher.group(8));
        long bytesSent = Long.parseLong(matcher.group(9));

        LogEntry logEntry = new LogEntry.Builder()
                .ipAddress(ipAddress)
                .dateTime(dateTime)
                .method(method)
                .uri(uri)
                .protocol(protocol)
                .status(status)
                .bytesSent(bytesSent)
                .build();
        return logEntry;
    }

    public static void main(String[] args) {
        String logEntry = "192.168.0.1 - - [25/May/2022:10:15:32 -0400] \"GET /index.html HTTP/1.1\" 200 1234";
        parseLogEntry(logEntry);
    }
}

