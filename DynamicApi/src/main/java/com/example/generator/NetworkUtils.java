package com.example.generator;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Random;


public class NetworkUtils {

    public NetworkUtils() {
    }



    // Method to get the system IP address
    public static String getSystemIpAddress() {
        try {
            InetAddress inetAddress = InetAddress.getLocalHost();
            return inetAddress.getHostAddress();  // Returns the local IP
        } catch (UnknownHostException e) {
            e.printStackTrace();
            return "Unable to get IP";
        }
    }

    // Method to generate a random MAC address
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
