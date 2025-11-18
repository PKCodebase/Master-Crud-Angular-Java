package com.example.demo;

import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.stream.Collectors;

import com.example.security.IPUtil;
import jakarta.servlet.http.HttpServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import com.example.service.DatabaseMetadataService;

@CrossOrigin
@RestController
@RequestMapping("/dynamicApi")
public class DynamicCrudApiController {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private DatabaseMetadataService metadataService;

    private static final Logger log = LoggerFactory.getLogger(DynamicCrudApiController.class);


    @GetMapping("/hello")
    public String hello() {
        return "Hello, Spring Boot!";
    }

    @GetMapping("/check/{table}/{column}")
    public ResponseEntity<List<String>> getCheckDropdown(
            @PathVariable String table,
            @PathVariable String column) {

        List<String> values = metadataService.getCheckConstraintValues(table, column);
        System.out.println("API Response for " + column + " → " + values);
        return ResponseEntity.ok(values);
    }

    // List tables per schema
    @GetMapping("/tables")
    public Map<String, List<String>> listTables() throws SQLException {
        String schemas = getProperty("valid.schema.list");
        List<String> schemaList = new ArrayList<>(Arrays.asList(schemas.split(",")));
        return metadataService.getAllTables(schemaList);
    }

    // Get all rows
    @GetMapping("/{schema}/{table}")
    public ResponseEntity<?> getAll(
            @PathVariable String schema,
            @PathVariable String table) throws SQLException {

        long start = System.currentTimeMillis();
        log.info("Fetching all records from {}.{}", schema, table);

        // ---------------------------- VALIDATION ----------------------------
        if (schema == null || schema.isBlank() || table == null || table.isBlank()) {
            log.warn("Invalid request: schema='{}', table='{}'", schema, table);
            return ResponseEntity
                    .status(HttpStatus.BAD_REQUEST)
                    .body(Map.of("error", "Schema or table name cannot be blank"));
        }

        if (!getValidSchemaList(schema)) {
            log.warn("Schema '{}' is not allowed", schema);
            return ResponseEntity
                    .status(HttpStatus.BAD_REQUEST)
                    .body(Map.of("error", "Enter valid schema"));
        }

        try {
            // ------------------------- GET COLUMNS -------------------------
            List<Map<String, Object>> columns = metadataService.getColumns(schema, table);
            log.debug("Column metadata loaded for {}.{}", schema, table);

            // ------------------------- FETCH DATA --------------------------
            String sql = String.format("SELECT * FROM %s.%s", schema, table);
            List<Map<String, Object>> rows = jdbcTemplate.queryForList(sql);
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");

            // ------------------------- PROCESS ROWS ------------------------
            for (Map<String, Object> row : rows) {
                for (Map<String, Object> col : columns) {

                    String colName = col.get("name").toString();
                    String colType = col.get("type").toString().toLowerCase();

                    Object value = row.get(colName);
                    if (value == null) continue;

                    // Normalize timestamp columns
                    if (colType.contains("timestamp")) {
                        try {
                            if (value instanceof Timestamp ts) {
                                row.put(colName, ts.toLocalDateTime().format(formatter));
                            } else if (value instanceof java.time.OffsetDateTime odt) {
                                row.put(colName, odt.toLocalDateTime().format(formatter));
                            } else {
                                row.put(colName, LocalDateTime.parse(value.toString()).format(formatter));
                            }
                        } catch (Exception e) {
                            log.error("Failed to normalize timestamp for {}.{}: value='{}', error={}",
                                    schema, table, value, e.getMessage());
                        }
                    }

                    // Uppercase any CODE columns
                    if (colName.toLowerCase().contains("code")) {
                        try {
                            row.put(colName, value.toString().toUpperCase());
                        } catch (Exception e) {
                            log.warn("Failed to uppercase code column '{}' in {}.{}", colName, schema, table);
                        }
                    }
                }
            }

            long duration = System.currentTimeMillis() - start;
            log.info("Fetched {} rows from {}.{} in {} ms", rows.size(), schema, table, duration);

            return ResponseEntity.ok(rows);
        }
        catch (Exception ex) {
            log.error("Error fetching data from {}.{}: {}", schema, table, ex.getMessage(), ex);
            return ResponseEntity
                    .status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", "Failed to fetch data", "details", ex.getMessage()));
        }
    }


    @GetMapping("/{schema}/{table}/columns")
    public List<Map<String, Object>> getTableColumns(
            @PathVariable String schema,
            @PathVariable String table) throws SQLException {
        return metadataService.getColumns(schema, table);
    }

    // Get by primary key automatically
    @GetMapping("/{schema}/{table}/{id}")
    public List<Map<String, Object>> getById(
            @PathVariable String schema,
            @PathVariable String table,
            @PathVariable String id) throws SQLException {

        List<String> pkCols = metadataService.getPrimaryKeys(schema, table);
        if (pkCols.isEmpty()) {
            throw new RuntimeException("No primary key defined for " + schema + "." + table);
        }
        String pk = pkCols.get(0); // support single PK for now

        String sql = "SELECT * FROM " + schema + "." + table + " WHERE " + pk + " = ?";
        List<Map<String, Object>> rows = jdbcTemplate.queryForList(sql, id);

        // Uppercase code fields and normalize timestamps similar to getAll
        List<Map<String, Object>> columns = metadataService.getColumns(schema, table);
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");

        for (Map<String, Object> row : rows) {
            for (Map<String, Object> col : columns) {
                String colName = col.get("name").toString();
                String colType = col.get("type").toString().toLowerCase();

                if (row.containsKey(colName) && row.get(colName) != null) {
                    Object val = row.get(colName);

                    if (colType.contains("timestamp")) {
                        if (val instanceof java.sql.Timestamp ts) {
                            row.put(colName, ts.toLocalDateTime().format(formatter));
                        } else if (val instanceof java.time.OffsetDateTime odt) {
                            row.put(colName, odt.toLocalDateTime().format(formatter));
                        } else {
                            try {
                                row.put(colName, LocalDateTime.parse(val.toString()).format(formatter));
                            } catch (Exception e) {
                                log.error("Exception--->" + e);
                            }
                        }
                    }

                    if (colName.toLowerCase().contains("code") && row.get(colName) != null) {
                        row.put(colName, row.get(colName).toString().toUpperCase());
                    }
                }
            }
        }

        return rows;
    }

    // Dynamic Search
    @GetMapping("/{schema}/{table}/search")
    public List<Map<String, Object>> search(
            @PathVariable String schema,
            @PathVariable String table,
            @RequestParam MultiValueMap<String, String> filterParams) {

        StringBuilder sql = new StringBuilder("SELECT * FROM ")
                .append(schema).append(".").append(table);

        List<Object> values = new ArrayList<>();

        if (!filterParams.isEmpty()) {
            sql.append(" WHERE ");
            List<String> conditions = new ArrayList<>();

            // get column types from DatabaseMetadataService
            Map<String, Integer> columnTypes = metadataService.getColumnTypes(schema, table);

            for (Map.Entry<String, List<String>> entry : filterParams.entrySet()) {
                String column = entry.getKey();

                for (String rawValue : entry.getValue()) {
                    String operator = "=";
                    String value = rawValue;

                    if (rawValue.startsWith(">=")) {
                        operator = ">=";
                        value = rawValue.substring(2);
                    } else if (rawValue.startsWith("<=")) {
                        operator = "<=";
                        value = rawValue.substring(2);
                    } else if (rawValue.startsWith(">")) {
                        operator = ">";
                        value = rawValue.substring(1);
                    } else if (rawValue.startsWith("<")) {
                        operator = "<";
                        value = rawValue.substring(1);
                    } else if (rawValue.startsWith("!=")) {
                        operator = "!=";
                        value = rawValue.substring(2);
                    }

                    int sqlType = columnTypes.getOrDefault(column, java.sql.Types.VARCHAR);
                    Object typedValue;

                    // CASE-INSENSITIVE SEARCH FOR CODE FIELDS
                    if (column.toLowerCase().contains("code")) {
                        conditions.add("UPPER(" + column + ") " + operator + " ?");
                        typedValue = value.toUpperCase();
                    } else if (sqlType == java.sql.Types.INTEGER || sqlType == java.sql.Types.BIGINT) {
                        conditions.add(column + " " + operator + " ?");
                        typedValue = Long.parseLong(value);
                    } else if (sqlType == java.sql.Types.DECIMAL || sqlType == java.sql.Types.NUMERIC) {
                        conditions.add(column + " " + operator + " ?");
                        typedValue = new java.math.BigDecimal(value);
                    } else {
                        conditions.add(column + " " + operator + " ?");
                        typedValue = value;
                    }

                    values.add(typedValue);
                }
            }

            sql.append(String.join(" AND ", conditions));
        }

        return jdbcTemplate.queryForList(sql.toString(), values.toArray());
    }

    @PostMapping("/{schema}/{table}")
    public ResponseEntity<?> insertRow(
            @PathVariable String schema,
            @PathVariable String table,
            @RequestBody Map<String, Object> rowData,
            HttpServletRequest request) throws SQLException {

        long start = System.currentTimeMillis();
        log.info("Insert request received for {}.{} | Payload={}", schema, table, rowData);

        List<Map<String, Object>> columns = metadataService.getColumns(schema, table);
        List<String> pkColumns = metadataService.getPrimaryKeys(schema, table);

        // ----------------- FOREIGN KEY VALIDATION -----------------
        List<Map<String, Object>> foreignKeys = metadataService.getForeignKeys(schema, table);

        for (Map<String, Object> fk : foreignKeys) {

            String fkColumn = fk.get("fkColumn").toString();     // e.g., ward_guid
            String pkTable = fk.get("pkTable").toString();       // table name
            String pkTableSchema = fk.get("pkTableSchema").toString();
            String pkColumn = fk.get("pkColumn").toString();     // ward_guid

            Object fkValue = rowData.get(fkColumn);

            // 1️⃣ FK missing
            if (fkValue == null || fkValue.toString().trim().isEmpty()) {
                return ResponseEntity
                        .status(HttpStatus.NOT_FOUND)
                        .body(Map.of(
                                "status", "error",
                                "timestamp", LocalDateTime.now().toString(),
                                "uri", request.getRequestURI(),
                                "message", fkColumn + " (foreign key) is missing"
                        ));
            }

            // 2️⃣ Invalid GUID format
            String fkString = fkValue.toString();
            try {
                UUID.fromString(fkString);
            } catch (Exception e) {
                log.error("Invalid fk string: " + fkString);
                return ResponseEntity
                        .status(HttpStatus.NOT_FOUND)
                        .body(Map.of(
                                "status", "error",
                                "timestamp", LocalDateTime.now().toString(),
                                "uri", request.getRequestURI(),
                                "message", fkColumn + " (foreign key) format is invalid: " + fkString
                        ));
            }

            // 3️⃣ Check if FK exists in parent table
            String sqlFkCheck = "SELECT COUNT(*) FROM " + pkTableSchema + "." + pkTable +
                    " WHERE " + pkColumn + " = ?";

            int countFk = jdbcTemplate.queryForObject(sqlFkCheck, Integer.class, fkString);

            if (countFk == 0) {
                log.warn("Foreign key {} value '{}' not found in {}.{}",
                        fkColumn, fkString, pkTableSchema, pkTable);
                return ResponseEntity
                        .status(HttpStatus.NOT_FOUND)
                        .body(Map.of(
                                "status", "error",
                                "timestamp", LocalDateTime.now().toString(),
                                "uri", request.getRequestURI(),
                                "message", fkColumn + " is  incorrect."
                        ));
            }
        }
        // ----------------- UNIQUE CODE VALIDATION -----------------
        for (Map<String, Object> col : columns) {

            String colName = col.get("name").toString().toLowerCase();
            String colType = col.get("type").toString().toLowerCase();

            // Only validate if column name ends with _code
            if (!colName.endsWith("_code")) continue;

            // Only validate text/varchar columns
            if (!(colType.contains("char") || colType.contains("text") || colType.contains("varchar"))) {
                continue;
            }

            // Skip if no value provided
            if (!rowData.containsKey(colName)) continue;

            String value = rowData.get(colName).toString().trim().toUpperCase();
            if (value.isEmpty()) continue;

            if (isDuplicateCode(schema, table, colName, value)) {
                return ResponseEntity
                        .status(HttpStatus.CONFLICT)
                        .body(Map.of(
                                "status", "error",
                                "timestamp", LocalDateTime.now().toString(),
                                "uri", request.getRequestURI(),
                                "message", colName + " already exists: " + value
                        ));
            }
        }


        // ----------------- SYSTEM FIELDS -----------------
        rowData.put("created_by", "System");
        rowData.put("created_date", LocalDateTime.now());
        rowData.put("created_uri", request.getRequestURL().toString());
        rowData.put("created_ip_addr", IPUtil.getClientIp(request));
        rowData.put("api_service_url", request.getRequestURL());
        rowData.putIfAbsent("status", "ACTIVE");

        Map<String, Object> finalData = new HashMap<>(rowData);

        List<String> insertCols = new ArrayList<>();
        List<Object> values = new ArrayList<>();

        for (Map<String, Object> col : columns) {
            String colName = col.get("name").toString();
            String type = col.get("type").toString();

            if (pkColumns.contains(colName)) continue;

            if (finalData.containsKey(colName)) {
                Object val = finalData.get(colName);
                if (val != null && !val.toString().trim().isEmpty()) {
                    insertCols.add(colName);
                    values.add(convertValue(val, type, colName));
                }
            }
        }

        if (insertCols.isEmpty()) {
            return ResponseEntity
                    .status(HttpStatus.BAD_REQUEST)
                    .body(Map.of(
                            "status", "error",
                            "timestamp", LocalDateTime.now().toString(),
                            "uri", request.getRequestURI(),
                            "message", "No valid columns found"
                    ));
        }

        String sql = "INSERT INTO " + schema + "." + table +
                " (" + String.join(", ", insertCols) + ") VALUES (" +
                String.join(", ", Collections.nCopies(insertCols.size(), "?")) + ")";

        try {
            jdbcTemplate.update(sql, values.toArray());
            log.info("Insert successfully for {}.{} | {} ms", schema, table, (System.currentTimeMillis() - start));

            return ResponseEntity.ok(Map.of(
                    "status", "success",
                    "message", "Row inserted successfully"
            ));

        } catch (Exception ex) {
            log.error("Insert failed {}.{} : {}", schema, table, ex.getMessage(), ex);
            return ResponseEntity
                    .status(HttpStatus.CONFLICT)
                    .body(Map.of(
                            "status", "error",
                            "timestamp", LocalDateTime.now().toString(),
                            "uri", request.getRequestURI(),
                            "message", "Insert failed: " + ex.getMessage()
                    ));
        }
    }



    private boolean isDuplicateCode(String schema, String table, String colName, String value) {

        String sql = "SELECT EXISTS (" +
                "SELECT 1 FROM " + schema + "." + table +
                " WHERE UPPER(" + colName + ") = ?" +
                ")";

        boolean exists = Boolean.TRUE.equals(
                jdbcTemplate.queryForObject(sql, Boolean.class, value.toUpperCase())
        );

        log.debug("Duplicate check for {}.{} -> {}={}, exists={}",
                schema, table, colName, value, exists);

        return exists;
    }




    @PutMapping("/{schema}/{table}/{id}")
    public ResponseEntity<?> updateRow(
            @PathVariable String schema,
            @PathVariable String table,
            @PathVariable String id,
            @RequestBody Map<String, Object> rowData,
            HttpServletRequest request) throws SQLException {

        long start = System.currentTimeMillis();
        log.info("Update request for {}.{} | ID={} | Payload={}", schema, table, id, rowData);

        List<Map<String, Object>> columns = metadataService.getColumns(schema, table);
        List<String> pkColumns = metadataService.getPrimaryKeys(schema, table);

        if (pkColumns.isEmpty()) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(Map.of(
                    "status", "error",
                    "timestamp", LocalDateTime.now().toString(),
                    "uri", request.getRequestURI(),
                    "message", "Primary key not found"
            ));
        }

        String pk = pkColumns.get(0);

        // ---------------------- 1️⃣ CHECK IF PK IS VALID GUID ----------------------
        try {
            UUID.fromString(id);
        } catch (Exception ex) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of(
                    "status", "error",
                    "timestamp", LocalDateTime.now().toString(),
                    "uri", request.getRequestURI(),
                    "message", pk + " is Not Found: " + id
            ));
        }

        // ---------------------- 2️⃣ CHECK IF RECORD EXISTS ----------------------
//        String sqlCheck = "SELECT COUNT(*) FROM " + schema + "." + table + " WHERE " + pk + " = ?";
//        int exists = jdbcTemplate.queryForObject(sqlCheck, Integer.class, id);
//
//        if (exists == 0) {
//            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of(
//                    "status", "error",
//                    "timestamp", LocalDateTime.now().toString(),
//                    "uri", request.getRequestURI(),
//                    "message", "Guid not found for ID: " + id
//            ));
//        }
        // ---------------------- 2️⃣ CHECK IF RECORD EXISTS ----------------------
        String sqlCheck = "SELECT COUNT(*) FROM " + schema + "." + table +
                " WHERE LOWER(" + pk + ") = LOWER(?)";

        int exists = jdbcTemplate.queryForObject(sqlCheck, Integer.class, id.trim());

        if (exists == 0) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of(
                    "status", "error",
                    "timestamp", LocalDateTime.now().toString(),
                    "uri", request.getRequestURI(),
                    "message", "Guid not found for ID: " + id
            ));
        }


        // ---------------------- 3️⃣ FOREIGN KEY VALIDATION ----------------------
        List<Map<String, Object>> foreignKeys = metadataService.getForeignKeys(schema, table);

        for (Map<String, Object> fk : foreignKeys) {

            String fkColumn = fk.get("fkColumn").toString();
            String pkTable = fk.get("pkTable").toString();
            String pkTableSchema = fk.get("pkTableSchema").toString();
            String pkColumn = fk.get("pkColumn").toString();

            if (rowData.containsKey(fkColumn)) {

                Object fkValue = rowData.get(fkColumn);

                // Missing FK
                if (fkValue == null || fkValue.toString().trim().isEmpty()) {
                    return ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of(
                            "status", "error",
                            "timestamp", LocalDateTime.now().toString(),
                            "uri", request.getRequestURI(),
                            "message", fkColumn + " (foreign key) is missing!"
                    ));
                }

                // Invalid GUID
                try {
                    UUID.fromString(fkValue.toString());
                } catch (Exception e) {
                    return ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of(
                            "status", "error",
                            "timestamp", LocalDateTime.now().toString(),
                            "uri", request.getRequestURI(),
                            "message", fkColumn + " (foreign key) format is invalid: " + fkValue
                    ));
                }

                // Check FK exists in parent table
                String sqlFkCheck = "SELECT COUNT(*) FROM " + pkTableSchema + "." + pkTable +
                        " WHERE " + pkColumn + " = ?";

                int parentExists = jdbcTemplate.queryForObject(sqlFkCheck, Integer.class, fkValue.toString());

                if (parentExists == 0) {
                    return ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of(
                            "status", "error",
                            "timestamp", LocalDateTime.now().toString(),
                            "uri", request.getRequestURI(),
                            "message", fkColumn + " does not found : "+fkValue
                    ));
                }
            }
        }

        // ---------------------- 4️⃣ DUPLICATE CODE VALIDATION ----------------------
//        for (Map<String, Object> col : columns) {
//
//            String colName = col.get("name").toString();
//
//            if (colName.toLowerCase().contains("code") && rowData.containsKey(colName)) {
//
//                String newValue = rowData.get(colName).toString().toUpperCase();
//
//                String sql = "SELECT COUNT(*) FROM " + schema + "." + table +
//                        " WHERE UPPER(" + colName + ") = ? AND " + pk + " <> ?";
//
//                int count = jdbcTemplate.queryForObject(sql, Integer.class, newValue, id);
//
//                if (count > 0) {
//                    log.warn("Duplicate {} detected during update: {}", colName, newValue);
//
//                    return ResponseEntity.status(HttpStatus.CONFLICT).body(Map.of(
//                            "status", "error",
//                            "timestamp", LocalDateTime.now().toString(),
//                            "uri", request.getRequestURI(),
//                            "message", colName + " already exists: " + newValue
//                    ));
//                }
//            }
//        }

        // ----------------- UNIQUE CODE VALIDATION -----------------
        for (Map<String, Object> col : columns) {

            String colName = col.get("name").toString().toLowerCase();

            // Only check for exact *_code fields
            if (!colName.endsWith("_code")) continue;

            if (rowData.containsKey(col.get("name"))) {

                String value = rowData.get(col.get("name")).toString().toUpperCase();

                if (isDuplicateCode(schema, table, (String) col.get("name"), value)) {
                    return ResponseEntity.status(HttpStatus.CONFLICT).body(Map.of(
                            "status", "error",
                            "timestamp", LocalDateTime.now().toString(),
                            "uri", request.getRequestURI(),
                            "message", col.get("name") + " already exists: " + value
                    ));
                }
            }
        }


        // ---------------------- 5️⃣ SYSTEM FIELDS ----------------------
        rowData.put("modified_by", "System");
        rowData.put("modified_date", LocalDateTime.now());
        rowData.put("modified_ip_addr", IPUtil.getClientIp(request));
        rowData.put("modified_uri", request.getRequestURL().toString());
        rowData.put("api_service_url", request.getRequestURL());

        // ---------------------- 6️⃣ BUILD UPDATE QUERY ----------------------
        List<String> updateCols = new ArrayList<>();
        List<Object> values = new ArrayList<>();

        for (Map<String, Object> col : columns) {

            String colName = col.get("name").toString();
            String type = col.get("type").toString();

            if (pk.equals(colName)) continue;

            if (rowData.containsKey(colName)) {
                Object val = rowData.get(colName);
                if (val != null && !val.toString().trim().isEmpty()) {
                    updateCols.add(colName + " = ?");
                    values.add(convertValue(val, type, colName));
                }
            }
        }

        if (updateCols.isEmpty()) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(Map.of(
                    "status", "error",
                    "timestamp", LocalDateTime.now().toString(),
                    "uri", request.getRequestURI(),
                    "message", "No updatable columns provided"
            ));
        }

        values.add(id);

        String sql = "UPDATE " + schema + "." + table +
                " SET " + String.join(", ", updateCols) +
                " WHERE " + pk + " = ?";


        try {
            jdbcTemplate.update(sql, values.toArray());
            log.info("Raw update successfully : {} ", schema, table, id, rowData);

            return ResponseEntity.ok(Map.of(
                    "status", "success",
                    "message", "Row updated successfully"
            ));

        } catch (Exception ex) {

            log.error("Update failed for {}.{} | ID={} : {}", schema, table, id, ex.getMessage(), ex);

            return ResponseEntity.status(HttpStatus.CONFLICT).body(Map.of(
                    "status", "error",
                    "timestamp", LocalDateTime.now().toString(),
                    "uri", request.getRequestURI(),
                    "message", ex.getMessage()
            ));
        }
    }




    private String getColumnType(List<Map<String, Object>> columns, String columnName) {
        return columns.stream()
                .filter(c -> columnName.equalsIgnoreCase((String) c.get("name")))
                .map(c -> (String) c.get("type"))
                .findFirst()
                .orElse("text");
    }

    private Object convertValue(Object value, String dataType, String columnName) {
        if (value == null) return null;

        String str = value.toString().trim();

        // ⭐ Auto UPPERCASE for any "code" field
        if (columnName != null && columnName.toLowerCase().contains("code")) {
            str = str.toUpperCase();
        }

        switch (dataType.toLowerCase()) {
            case "bigserial":
            case "bigint":
            case "integer":
            case "smallint":
                return Long.valueOf(str);

            case "numeric":
            case "decimal":
                return new java.math.BigDecimal(str);

            case "bool":
            case "boolean":
                return Boolean.valueOf(str);

            case "date":
                return java.sql.Date.valueOf(str);

            case "json":
            case "jsonb":
                try {
                    org.postgresql.util.PGobject jsonObject = new org.postgresql.util.PGobject();
                    jsonObject.setType("jsonb");

                    String jsonString = str.trim();

                    // ✅ Handle cases where user typed plain text like abc or [1,2,3] or {"key":"value"}
                    if (jsonString.startsWith("{") || jsonString.startsWith("[") || jsonString.equalsIgnoreCase("null")) {
                        // valid JSON string — use as-is
                        jsonObject.setValue(jsonString);
                    } else {
                        // Not valid JSON → wrap as quoted string
                        jsonObject.setValue("\"" + jsonString.replace("\"", "\\\"") + "\"");
                    }

                    return jsonObject;
                } catch (Exception e) {
                    throw new RuntimeException("Failed to convert value to JSONB: " + str, e);
                }

            case "timestamp":
            case "timestamptz":
            case "timestamp with time zone":
                try {
                    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");
                    String cleanedStr = str;
                    int dotIndex = str.indexOf(".");
                    if (dotIndex != -1) {
                        cleanedStr = str.substring(0, dotIndex);
                    }
                    LocalDateTime ldt = LocalDateTime.parse(cleanedStr, formatter);
                    return Timestamp.valueOf(ldt);
                } catch (DateTimeParseException e) {
                    return Timestamp.valueOf(str.replace("T", " "));
                }

            case "time":
            case "timetz":
            case "time with time zone":
                if (str.contains("+")) {
                    return Time.valueOf(str.split("\\+")[0]);
                } else {
                    return Time.valueOf(str);
                }

            default:
                return str; // fallback: varchar, text, etc.
        }
    }

    // Delete by PK
    @DeleteMapping("/{schema}/{table}/{id}")
    public int deleteRow(
            @PathVariable String schema,
            @PathVariable String table,
            @PathVariable String id) throws SQLException {

        List<String> pkCols = metadataService.getPrimaryKeys(schema, table);
        if (pkCols.isEmpty()) {
            throw new RuntimeException("No primary key defined for " + schema + "." + table);
        }
        String pk = pkCols.get(0);

        String sql = "DELETE FROM " + schema + "." + table + " WHERE " + pk + " = ?";
        return jdbcTemplate.update(sql, id);
    }

    @GetMapping("/{schema}/{table}/fk-values/{column}")
    public List<Map<String, Object>> getForeignKeyValues(
            @PathVariable String schema,
            @PathVariable String table,
            @PathVariable String column) throws SQLException {

        List<Map<String, Object>> fks = metadataService.getForeignKeys(schema, table);
        Map<String, Object> fkInfo = fks.stream()
                .filter(fk -> fk.get("fkColumn").equals(column))
                .findFirst()
                .orElseThrow(() -> new RuntimeException("No FK found for column " + column));

        String pkTable = (String) fkInfo.get("pkTable");
        String pkTableSchema = (String) fkInfo.get("pkTableSchema");
        String pkColumn = (String) fkInfo.get("pkColumn");
        String displayColumn = (String) fkInfo.get("displayColumn");

        if (displayColumn.equalsIgnoreCase("NONE")) {
            displayColumn = pkColumn; // fallback
        }

        String sql = "SELECT " + pkColumn + " as id, " + displayColumn + " as value FROM "
                + pkTableSchema + "." + pkTable;

        return jdbcTemplate.queryForList(sql);
    }

    public String getDropdownColumnsForForeignKeys(String schema, String table) {
        try {
            if (schema.isBlank() && table.isBlank()) {
                return "";
            }
            if (getValidSchemaList(schema)) {
                String tableName = schema + "." + table;
                String key = getProperty(tableName + ".key");
                String val = getProperty(tableName + ".val");

                if (!key.isBlank() && !val.isBlank()) {
                    return key + " as id, " + val + " as value ";
                } else {
                    log.error("Key and Value unavailable for table ---" + tableName);
                }
            } else {
                log.error("Invalid Schema -- " + schema);
            }
        } catch (Exception e) {
            log.error("Exception--->" + e);
        }
        return "";
    }

    public boolean getValidSchemaList(String schema) {
        log.info("Validating schema: {}", schema);

        if (schema == null || schema.isBlank()) {
            log.warn("Schema validation failed: Provided schema is null or blank");
            return false;
        }

        try {
            String schemas = getProperty("valid.schema.list");

            if (schemas == null || schemas.isBlank()) {
                log.error("Schema validation failed: 'valid.schema.list' property is missing or empty");
                return false;
            }

            List<String> schemaList = Arrays.stream(schemas.split(","))
                    .map(String::trim)
                    .filter(s -> !s.isEmpty())
                    .distinct()  // avoid duplicates
                    .collect(Collectors.toList());

            boolean isValid = schemaList.contains(schema);

            if (isValid) {
                log.info("Schema '{}' validated successfully", schema);
            } else {
                log.warn("Schema '{}' is not listed in valid.schema.list: {}", schema, schemaList);
            }

            return isValid;
        }
        catch (Exception ex) {
            log.error("Unexpected error validating schema '{}': {}", schema, ex.getMessage(), ex);
            return false;
        }
    }


    public String getProperty(String val) {
        String propValue = "";
        try {
            Properties props = new Properties();
            props.load(new ClassPathResource("tables-dropdown.properties").getInputStream());
            propValue = props.getProperty(val);
            if (!propValue.isBlank()) {
                return propValue;
            }
        } catch (Exception e) {
            log.error("Exception--->" + e);
        }
        return propValue;
    }

    @GetMapping("/{schema}/{table}/constraints")
    public List<Map<String, Object>> getConstraints(@PathVariable String schema, @PathVariable String table) {
        String sql = """
            SELECT conname, pg_get_constraintdef(c.oid) as definition
            FROM pg_constraint c
            JOIN pg_class t ON c.conrelid = t.oid
            JOIN pg_namespace n ON n.oid = t.relnamespace
            WHERE n.nspname = ? AND t.relname = ?
        """;
        return jdbcTemplate.queryForList(sql, schema, table);
    }

    // ✅ API to fetch all schemas
    @GetMapping("/schemas")
    public ResponseEntity<List<String>> getAllSchemas() {
        return ResponseEntity.ok(metadataService.getAllSchemas());
    }

    // ✅ API to fetch tables by schema
    @GetMapping("/tables/{schema}")
    public ResponseEntity<List<String>> getTables(@PathVariable String schema) {
        return ResponseEntity.ok(metadataService.getTablesBySchema(schema));
    }

}
