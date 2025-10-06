//package com.example.config;
//
//import org.springframework.beans.factory.annotation.Value;
//import org.springframework.context.annotation.Configuration;
//
//import java.util.HashMap;
//import java.util.Map;
//
//@Configuration
//public class ForeignKeyConfig {
//
//    private final Map<String, String> keyColumns = new HashMap<>();
//    private final Map<String, String> valColumns = new HashMap<>();
//
//    public ForeignKeyConfig(@Value("#{${fk.config.map}}") Map<String, String> fkConfig) {
//        for (Map.Entry<String, String> entry : fkConfig.entrySet()) {
//            String fullKey = entry.getKey();   // e.g. mst.colony.key
//            String value = entry.getValue();   // e.g. ward_guid / ward_name_en
//
//            String[] parts = fullKey.split("\\.");
//            if (parts.length == 3) {
//                String schema = parts[0];
//                String table = parts[1];
//                String type = parts[2]; // key OR val
//                String identifier = schema + "." + table;
//
//                if ("key".equals(type)) {
//                    keyColumns.put(identifier, value);
//                } else if ("val".equals(type)) {
//                    valColumns.put(identifier, value);
//                }
//            }
//        }
//    }
//
//    public String getKeyColumn(String schema, String table) {
//        return keyColumns.get(schema + "." + table);
//    }
//
//    public String getValColumn(String schema, String table) {
//        return valColumns.get(schema + "." + table);
//    }
//}
