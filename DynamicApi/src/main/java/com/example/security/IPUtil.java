package com.example.security;

import jakarta.servlet.http.HttpServletRequest;

import java.util.Random;

public class IPUtil {
	
	public static String getClientIp(HttpServletRequest request) {
        String ip = request.getHeader("X-Forwarded-For");
        if (ip == null || ip.isEmpty() || "unknown".equalsIgnoreCase(ip)) {
            ip = request.getRemoteAddr();
        }
        return ip;
    }

    public static String generateRandomMacAddress() {
        Random random = new Random();
        byte[] macAddr = new byte[6];
        random.nextBytes(macAddr);

        // Set the locally administered bit (unicast)
        macAddr[0] = (byte) (macAddr[0] & (byte) 254);

        StringBuilder macAddress = new StringBuilder();
        for (byte b : macAddr) {
            if (macAddress.length() > 0) macAddress.append(":");
            macAddress.append(String.format("%02X", b));
        }
        return macAddress.toString();
    }
}
