package com.example.service;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

@Service
@PropertySource("classpath:tables-dropdown.properties")
public class DatabaseMetadataService {

    @Autowired
    private JdbcTemplate jdbcTemplate;


    @Autowired
    private Environment env;

    private static final Logger log = LoggerFactory.getLogger(DatabaseMetadataService.class);



    /**
     * Get list of schemas and tables in the DB
     */
    public Map<String, List<String>> getAllTables() throws SQLException {
        Map<String, List<String>> schemaTables = new HashMap<>();
        try (Connection conn = Objects.requireNonNull(jdbcTemplate.getDataSource()).getConnection()) {
            DatabaseMetaData metaData = conn.getMetaData();
            ResultSet rs = metaData.getTables(null, null, "%", new String[]{"TABLE"});

            while (rs.next()) {
                String schema = rs.getString("TABLE_SCHEMA");
                String table = rs.getString("TABLE_NAME");
                schemaTables.computeIfAbsent(schema, k -> new ArrayList<>()).add(table);
            }
        }
        return schemaTables;
    }

    public Map<String, List<String>> getAllTables(List<String> schemaList) throws SQLException {
        Map<String, List<String>> schemaTables = new HashMap<>();
        try (Connection conn = Objects.requireNonNull(jdbcTemplate.getDataSource()).getConnection()) {
            DatabaseMetaData metaData = conn.getMetaData();
            ResultSet rs = metaData.getTables(null, null, "%", new String[]{"TABLE"});

            while (rs.next()) {
                String schema = rs.getString("TABLE_SCHEMA");

                if (!schemaList.isEmpty() && schemaList.contains(schema)) {
                    String table = rs.getString("TABLE_NAME");
                    schemaTables.computeIfAbsent(schema, k -> new ArrayList<>()).add(table);
                }
            }
        }
        return schemaTables;
    }

    public List<Map<String, Object>> getColumns(String schema, String table) throws SQLException {

        long start = System.currentTimeMillis();
        log.info("Fetching column metadata for {}.{}", schema, table);

        if (schema == null || schema.isBlank() || table == null || table.isBlank()) {
            log.warn("Invalid input: schema='{}' table='{}'", schema, table);
            return Collections.emptyList();
        }

        List<Map<String, Object>> columns = new ArrayList<>();

        try (Connection conn = Objects.requireNonNull(jdbcTemplate.getDataSource()).getConnection()) {

            DatabaseMetaData meta = conn.getMetaData();

            // --- Fetch PK ---
            Set<String> pkCols = new HashSet<>();
            try (ResultSet rs = meta.getPrimaryKeys(null, schema, table)) {
                while (rs.next()) {
                    pkCols.add(rs.getString("COLUMN_NAME"));
                }
            }
            log.debug(" Available PK columns for {}.{} => {}", schema, table, pkCols);

            // --- Fetch FK ---
            Set<String> fkCols = new HashSet<>();
            try (ResultSet rs = meta.getImportedKeys(conn.getCatalog(), schema, table)) {
                while (rs.next()) {
                    fkCols.add(rs.getString("FKCOLUMN_NAME"));
                }
            }
            log.debug("Available FK columns for {}.{} => {}", schema, table, fkCols);

            // --- Fetch Column Metadata ---
            try (ResultSet rs = meta.getColumns(null, schema, table, "%")) {
                while (rs.next()) {

                    String colName = rs.getString("COLUMN_NAME");
                    String rawType = rs.getString("TYPE_NAME").toLowerCase();

                    String normalizedType = switch (rawType) {
                        case "int2"     -> "smallint";
                        case "int4"     -> "integer";
                        case "int8"     -> "bigint";
                        case "serial"   -> "serial";
                        case "bigserial"-> "bigserial";
                        case "json", "jsonb" -> "json";
                        case "varchar"  -> "varchar";
                        case "bpchar"   -> "char";
                        case "timestamptz" -> "timestamp with time zone";
                        case "timestamp" -> "timestamp";
                        default -> rawType;
                    };

                    Map<String, Object> col = new HashMap<>();
                    col.put("name", colName);
                    col.put("type", normalizedType);
                    col.put("size", rs.getInt("COLUMN_SIZE"));
                    col.put("nullable", "YES".equalsIgnoreCase(rs.getString("IS_NULLABLE")));
                    col.put("autoIncrement", "YES".equalsIgnoreCase(rs.getString("IS_AUTOINCREMENT")));
                    col.put("primaryKey", pkCols.contains(colName));
                    col.put("isForeignKey", fkCols.contains(colName));

                    columns.add(col);
                }
            }

            log.info("Fetched {} columns for {}.{}", columns.size(), schema, table);

        } catch (Exception ex) {
            log.error("Error reading columns for {}.{} : {}", schema, table, ex.getMessage(), ex);
            throw ex;
        } finally {
            log.info("getColumns({}.{}) completed in {} ms",
                    schema, table, (System.currentTimeMillis() - start));
        }

        return columns;
    }

    public List<String> getPrimaryKeys(String schema, String table) throws SQLException {

        log.debug("Fetching primary keys for {}.{}", schema, table);

        List<String> pkCols = new ArrayList<>();

        try (Connection conn = Objects.requireNonNull(jdbcTemplate.getDataSource()).getConnection()) {

            DatabaseMetaData meta = conn.getMetaData();
            ResultSet rs = meta.getPrimaryKeys(null, schema, table);

            while (rs.next()) {
                pkCols.add(rs.getString("COLUMN_NAME"));
            }

            log.info("Found {} PK columns for {}.{} -> {}", pkCols.size(), schema, table, pkCols);

        } catch (Exception ex) {
            log.error("Failed to fetch PK for {}.{} : {}", schema, table, ex.getMessage(), ex);
            throw ex;
        }

        return pkCols;
    }


    //Exact

    public List<Map<String, Object>> getForeignKeys(String schema, String table) throws SQLException {
        List<Map<String, Object>> fks = new ArrayList<>();
        try (Connection conn = Objects.requireNonNull(jdbcTemplate.getDataSource()).getConnection()) {
            DatabaseMetaData metaData = conn.getMetaData();
            ResultSet rs = metaData.getImportedKeys(conn.getCatalog(), schema, table);

            while (rs.next()) {
                Map<String, Object> fk = new HashMap<>();
                fk.put("fkColumn", rs.getString("FKCOLUMN_NAME"));
                fk.put("pkTableSchema", rs.getString("PKTABLE_SCHEM"));
                fk.put("pkTable", rs.getString("PKTABLE_NAME"));
                fk.put("pkColumn", rs.getString("PKCOLUMN_NAME"));

                // 🔍 Identify display columns for referenced table
                List<String> displayCols = new ArrayList<>();
                try (ResultSet cols = metaData.getColumns(conn.getCatalog(), rs.getString("PKTABLE_SCHEM"), rs.getString("PKTABLE_NAME"), "%")) {
                    while (cols.next()) {
                        String colName = cols.getString("COLUMN_NAME").toLowerCase();
                        if (colName.contains("name_en")) {
                            displayCols.add(colName);
                        }else if (colName.contains("code")) {
                        	 displayCols.add(colName);
                        }
                    }
                    if(!displayCols.isEmpty()) {
                    	fk.put("displayColumn", displayCols.get(0));
                    }else {
                    	fk.put("displayColumn", "NONE");
                    }

                }
                //fk.put("displayColumns", displayCols);

                fks.add(fk);
            }
        }
        return fks;
    }

    public String getColumnType(String schema, String table, String column) {
        List<String> result = jdbcTemplate.query(
            "SELECT data_type FROM information_schema.columns " +
            "WHERE table_schema = ? AND table_name = ? AND column_name = ?",
            (rs, rowNum) -> rs.getString("data_type"),
            schema, table, column
        );
        return result.stream().findFirst().orElse(null);
    }

    public Object castValue(String value, String type) {
        if (type == null) return value;

        switch (type.toLowerCase()) {
            case "bigint":
            case "integer":
            case "smallint":
                return Long.valueOf(value);
            case "numeric":
            case "decimal":
            case "double precision":
            case "real":
                return Double.valueOf(value);
            case "boolean":
                return Boolean.valueOf(value);
            case "date":
                return java.sql.Date.valueOf(value); // yyyy-MM-dd
            case "timestamp":
            case "timestamp without time zone":
            case "timestamp with time zone":
                return java.sql.Timestamp.valueOf(value); // yyyy-MM-dd hh:mm:ss
            default:
                return value; // fallback: string
        }
    }



    public Map<String, Integer> getColumnTypes(String schema, String table) {
        String sql = """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = ? AND table_name = ?
        """;

        List<Map<String, Object>> rows = jdbcTemplate.queryForList(sql, schema, table);

        Map<String, Integer> columnTypes = new HashMap<>();
        for (Map<String, Object> row : rows) {
            String columnName = (String) row.get("column_name");
            String dataType = (String) row.get("data_type");

            int sqlType = mapPostgresTypeToSqlType(dataType);
            columnTypes.put(columnName, sqlType);
        }
        return columnTypes;
    }

    public List<Map<String, Object>> getColumnInfo(String schema, String table) {
        // Get all columns
        List<Map<String, Object>> cols = jdbcTemplate.query(
            "SELECT c.column_name, c.data_type, c.is_nullable, " +
            "       (CASE WHEN kcu.column_name IS NOT NULL THEN 'YES' ELSE 'NO' END) as is_primary_key " +
            "FROM information_schema.columns c " +
            "LEFT JOIN information_schema.key_column_usage kcu " +
            "       ON c.table_schema = kcu.table_schema " +
            "       AND c.table_name = kcu.table_name " +
            "       AND c.column_name = kcu.column_name " +
            "LEFT JOIN information_schema.table_constraints tc " +
            "       ON kcu.constraint_name = tc.constraint_name " +
            "       AND tc.constraint_type = 'PRIMARY KEY' " +
            "WHERE c.table_schema = ? AND c.table_name = ? " +
            "ORDER BY c.ordinal_position",
            new Object[]{schema, table},
            (rs, rowNum) -> {
                Map<String, Object> col = new HashMap<>();
                col.put("column_name", rs.getString("column_name"));
                col.put("data_type", rs.getString("data_type"));
                col.put("is_nullable", rs.getString("is_nullable"));
                col.put("is_primary_key", rs.getString("is_primary_key")); // YES/NO
                return col;
            }
        );
        return cols;
    }


    private int mapPostgresTypeToSqlType(String pgType) {
        return switch (pgType) {
            case "integer" -> java.sql.Types.INTEGER;
            case "bigint" -> java.sql.Types.BIGINT;
            case "numeric", "decimal" -> java.sql.Types.NUMERIC;
            case "double precision" -> java.sql.Types.DOUBLE;
            case "real" -> java.sql.Types.REAL;
            case "boolean" -> java.sql.Types.BOOLEAN;
            case "date" -> java.sql.Types.DATE;
            case "timestamp without time zone", "timestamp with time zone" -> java.sql.Types.TIMESTAMP;
            case "character varying", "varchar", "text" -> java.sql.Types.VARCHAR;
            default -> java.sql.Types.VARCHAR; // fallback
        };
    }

    public List<String> getAllSchemas() {
        log.info("Fetching all schemas from configuration");

        try {
            String schemas = env.getProperty("valid.schema.list");

            if (schemas == null || schemas.isBlank()) {
                log.warn("No schemas found in property: valid.schema.list");
                return Collections.emptyList();
            }

            List<String> schemaList = Arrays.stream(schemas.split(","))
                    .map(String::trim)
                    .filter(fetchedSchemas -> !fetchedSchemas.isEmpty())
                    .distinct()
                    .collect(Collectors.toUnmodifiableList()); // 🔥 immutable list (safer)

            log.info("Schemas fetched successfully: {}", schemaList);

            return schemaList;
        }
        catch (Exception ex) {
            log.error("Error occurred while fetching schemas: {}", ex.getMessage(), ex);
            return Collections.emptyList();
        }
    }


    public List<String> getTablesBySchema(String schema) {

        log.info("Fetching tables for schema: " + schema);

        // Fetch all tables for given schema
        String sql = "SELECT table_name FROM information_schema.tables WHERE table_schema = ? ORDER BY table_name";
        List<String> allTables = jdbcTemplate.queryForList(sql, new Object[]{schema}, String.class);

        // Check if there's a whitelist defined
        log.info("All tables before filtering: " + allTables);

        String allowedList = env.getProperty("allowed.tables." + schema, "").trim();

        log.info("Allowed tables: " + allowedList);

        if (!allowedList.isEmpty()) {
            List<String> allowed = Arrays.stream(allowedList.split(","))
                    .map(String::trim)
                    .filter(s -> !s.isEmpty())
                    .toList();

            // return only allowed tables
            return allTables.stream()
                    .filter(allowed::contains)
                    .toList();
        }

        //  Otherwise, check for blacklist
        String excludeList = env.getProperty("exclude.tables." + schema, "").trim();
        if (!excludeList.isEmpty()) {
            List<String> excluded = Arrays.stream(excludeList.split(","))
                    .map(String::trim)
                    .filter(s -> !s.isEmpty())
                    .toList();

            // return all except excluded
            return allTables.stream()
                    .filter(t -> !excluded.contains(t))
                    .toList();
        }

        // No filter defined → return all
        return allTables;
    }


        public List<String> getCheckConstraintValues(String tableName, String columnName) {
            List<String> values = new ArrayList<>();

            try {
                // Extract schema + table
                String[] parts = tableName.split("\\.");
                String schema = parts.length > 1 ? parts[0] : "public";
                String table = parts.length > 1 ? parts[1] : tableName;

                String sql = """
            SELECT pg_get_constraintdef(c.oid) AS constraint_def
            FROM pg_constraint c
            JOIN pg_class t ON c.conrelid = t.oid
            JOIN pg_namespace n ON n.oid = t.relnamespace
            WHERE n.nspname = ? AND t.relname = ? AND c.contype = 'c'
        """;

                List<String> defs = jdbcTemplate.queryForList(sql, new Object[]{schema, table}, String.class);

                for (String def : defs) {
                    if (def == null) continue;

                    if (def.toLowerCase().contains(columnName.toLowerCase())) {
                        // Handle ARRAY[...] or IN(...)
                        Pattern pattern = Pattern.compile("ARRAY\\[(.*?)\\]|IN\\s*\\((.*?)\\)", Pattern.CASE_INSENSITIVE);
                        Matcher matcher = pattern.matcher(def);

                        while (matcher.find()) {
                            String raw = matcher.group(1) != null ? matcher.group(1) : matcher.group(2);
                            if (raw == null) continue;

                            String[] arr = raw.split(",");
                            for (String item : arr) {
                                String clean = item
                                        .replaceAll("::[a-zA-Z_ ]+", "")
                                        .replace("(", "")
                                        .replace(")", "")
                                        .replace("'", "")
                                        .trim();

                                if (!clean.isEmpty()) values.add(clean);
                            }
                        }
                    }
                }

            } catch (Exception e) {
                e.printStackTrace();
            }

//            System.out.println("✅ Final check values = " + values);
            return values;
        }




}
