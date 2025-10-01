package com.example.service;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

@Service
public class DatabaseMetadataService {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    /**
     * Get list of schemas and tables in the DB
     */
    public Map<String, List<String>> getAllTables() throws SQLException {
        Map<String, List<String>> schemaTables = new HashMap<>();
        try (Connection conn = Objects.requireNonNull(jdbcTemplate.getDataSource()).getConnection()) {
            DatabaseMetaData metaData = conn.getMetaData();
            ResultSet rs = metaData.getTables(null, null, "%", new String[]{"TABLE"});

            while (rs.next()) {
                String schema = rs.getString("TABLE_SCHEM");
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
                String schema = rs.getString("TABLE_SCHEM");
                
                if(!schemaList.isEmpty() && schemaList.contains(schema)) {
                	String table = rs.getString("TABLE_NAME");
                    schemaTables.computeIfAbsent(schema, k -> new ArrayList<>()).add(table);
                }
            }
        }
        return schemaTables;
    }

    /**
     * Get columns for a given schema and table
     */
    public List<Map<String, Object>> getColumns(String schema, String table) throws SQLException {
    	List<Map<String, Object>> columns = new ArrayList<>();

        try (Connection conn = Objects.requireNonNull(jdbcTemplate.getDataSource()).getConnection()) {
            DatabaseMetaData metaData = conn.getMetaData();

            // Get primary key columns
            Set<String> pkColumns = new HashSet<>();
            try (ResultSet pkRs = metaData.getPrimaryKeys(null, schema, table)) {
                while (pkRs.next()) {
                    pkColumns.add(pkRs.getString("COLUMN_NAME"));
                }
            }
            
            // Collect FK columns for this table
            Set<String> fkColumns = new HashSet<>();
            ResultSet fkRs = metaData.getImportedKeys(conn.getCatalog(), schema, table);
            while (fkRs.next()) {
                fkColumns.add(fkRs.getString("FKCOLUMN_NAME"));
            }

            // 🔹 Get auto-increment columns (from ResultSet "IS_AUTOINCREMENT")
            try (ResultSet rs = metaData.getColumns(null, schema, table, "%")) {
                while (rs.next()) {
                    Map<String, Object> col = new HashMap<>();
                    String colName = rs.getString("COLUMN_NAME");
                    //String type = rs.getString("TYPE_NAME");
                    String rawType = rs.getString("TYPE_NAME").toLowerCase();
                 // 🔄 Normalize Postgres internal types
                    String normalizedType = switch (rawType) {
                        case "int2"     -> "smallint";
                        case "int4"     -> "integer";
                        case "int8"     -> "bigint";
                        case "serial"   -> "serial";
                        case "bigserial"-> "bigserial";
                        case "varchar"  -> "varchar";
                        case "bpchar"   -> "char";
                        case "timestamptz" -> "timestamp with time zone";
                        case "timestamp"   -> "timestamp";
                        default -> rawType; // fallback
                    };
                    
                    int size = rs.getInt("COLUMN_SIZE");
                    String isNullable = rs.getString("IS_NULLABLE");
                    String isAutoInc = rs.getString("IS_AUTOINCREMENT");

                    col.put("name", colName);
                    //col.put("type", type);
                    col.put("type", normalizedType);
                    col.put("size", size);
                    col.put("nullable", "YES".equalsIgnoreCase(isNullable));
                    col.put("autoIncrement", "YES".equalsIgnoreCase(isAutoInc));
                    col.put("primaryKey", pkColumns.contains(colName));
                    col.put("isForeignKey", fkColumns.contains(colName)); 

                    columns.add(col);
                }
            }
        }
        return columns;
    }

    /**
     * Get primary key column(s) for a given table
     */
    public List<String> getPrimaryKeys(String schema, String table) throws SQLException {
        List<String> pkCols = new ArrayList<>();
        try (Connection conn = Objects.requireNonNull(jdbcTemplate.getDataSource()).getConnection()) {
            DatabaseMetaData metaData = conn.getMetaData();
            ResultSet rs = metaData.getPrimaryKeys(null, schema, table);

            while (rs.next()) {
                pkCols.add(rs.getString("COLUMN_NAME"));
            }
        }
        return pkCols;
    }
    
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
        // PostgreSQL query to fetch all non-system schemas
        String sql = "SELECT schema_name FROM information_schema.schemata " +
                "WHERE schema_name NOT IN ('information_schema','pg_catalog') " +
                "ORDER BY schema_name";
        return jdbcTemplate.queryForList(sql, String.class);
    }


//    // ✅ Get all schemas
//    public List<String> getAllSchemas() {
//        String sql = "SELECT schema_name FROM information_schema.schemata ORDER BY schema_name";
//        return jdbcTemplate.queryForList(sql, String.class);
//    }

    // ✅ Get all tables for a given schema
    public List<String> getTablesBySchema(String schema) {
        String sql = "SELECT table_name FROM information_schema.tables WHERE table_schema = ? ORDER BY table_name";
        return jdbcTemplate.queryForList(sql, new Object[]{schema}, String.class);
    }
}
