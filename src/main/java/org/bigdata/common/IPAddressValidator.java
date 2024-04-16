package org.bigdata.common;

import java.util.regex.*;

public class IPAddressValidator {
    private static final String IPV4_PATTERN =
            "^((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$";

    private static final Pattern PATTERN = Pattern.compile(IPV4_PATTERN);

    public static boolean isValidIPv4(String ip) {
        Matcher matcher = PATTERN.matcher(ip);
        return matcher.matches();
    }

    public static void main(String[] args) {
        String ipAddress = "google.com";
        if (isValidIPv4(ipAddress)) {
            System.out.println(ipAddress + " is a valid IPv4 address.");
        } else {
            System.out.println(ipAddress + " is not a valid IPv4 address.");
        }
    }
}

