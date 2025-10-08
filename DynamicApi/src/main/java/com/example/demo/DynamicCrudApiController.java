package com.example.demo;

import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

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


//    private final NetworkUtils networkUtils;

    private static final Logger log = LoggerFactory.getLogger(DynamicCrudApiController.class);

//    public DynamicCrudApiController(NetworkUtils networkUtils) {
//        this.networkUtils = networkUtils;
//    }

    @GetMapping("/hello")
    public String hello() {
        return "Hello, Spring Boot!";
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
    	
    	Map<String, Object> error = new HashMap<>();
        if(schema.isBlank() && table.isBlank()) {
    		error.put("error", "Enter Schema");
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(error);
		}
    	
    	if(!getValidSchemaList(schema)) {
    		error.put("error", "Enter Valid Schema");
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(error);
    	}
    	
    	//return jdbcTemplate.queryForList("SELECT * FROM " + schema + "." + table);
    	List<Map<String, Object>> columns = metadataService.getColumns(schema, table);

        List<Map<String, Object>> rows = jdbcTemplate.queryForList(
                "SELECT * FROM " + schema + "." + table);

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");

        for (Map<String, Object> row : rows) {
            for (Map<String, Object> col : columns) {
                String colName = col.get("name").toString();
                String colType = col.get("type").toString().toLowerCase();

                if (row.containsKey(colName) && row.get(colName) != null) {
                    Object val = row.get(colName);

                    if (colType.contains("timestamp")) {
                        // Normalize to yyyy-MM-ddTHH:mm:ss
                        if (val instanceof java.sql.Timestamp ts) {
                            row.put(colName, ts.toLocalDateTime().format(formatter));
                        } else if (val instanceof java.time.OffsetDateTime odt) {
                            row.put(colName, odt.toLocalDateTime().format(formatter));
                        } else {
                            // fallback try parsing string
                            try {
                                row.put(colName, LocalDateTime.parse(val.toString()).format(formatter));
                            } catch (Exception e) {
                                // leave as-is if not parsable
                            	log.error("Exception--->"+e);
                            }
                        }
                    }
                }
            }
        }
        return ResponseEntity.ok(rows);
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
        return jdbcTemplate.queryForList(sql, id);
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

                    conditions.add(column + " " + operator + " ?");

                    // type-aware binding
                    int sqlType = columnTypes.getOrDefault(column, java.sql.Types.VARCHAR);
                    Object typedValue;

                    if (sqlType == java.sql.Types.INTEGER || sqlType == java.sql.Types.BIGINT) {
                        typedValue = Long.parseLong(value);
                    } else if (sqlType == java.sql.Types.DECIMAL || sqlType == java.sql.Types.NUMERIC) {
                        typedValue = new java.math.BigDecimal(value);
                    } else {
                        typedValue = value; // fallback string
                    }

                    values.add(typedValue);
                }
            }

            sql.append(String.join(" AND ", conditions));
        }

        return jdbcTemplate.queryForList(sql.toString(), values.toArray());
    }

    // Insert row
//    @PostMapping("/{schema}/{table}")
//    public Map<String, Object> insertRow(
//            @PathVariable String schema,
//            @PathVariable String table,
//            @RequestBody Map<String, Object> rowData) throws SQLException {
//
//    		List<Map<String, Object>> columns = metadataService.getColumns(schema, table);
//
//            // Identify primary keys
//            List<String> pkColumns = metadataService.getPrimaryKeys(schema, table);
//
//            // Build insertable columns (exclude serial/identity PKs)
//            List<String> insertableCols = new ArrayList<>();
//            List<Object> values = new ArrayList<>();
//
//            for (Map<String, Object> col : columns) {
////                String colName = (String) col.get("column_name");
////                String dataType = (String) col.get("data_type");
//            	String colName = (String) col.get("name");
//            	String dataType = (String) col.get("type");
//
//                if (pkColumns.contains(colName)) {
//                    // Skip PK if auto-generated
//                    continue;
//                }
//
//                if (rowData.containsKey(colName)) {
//                	Object val = rowData.get(colName);
//
//                    // Skip null or empty string
//                    if (val == null || val.toString().trim().isEmpty()) {
//                        continue;
//                    }
//
//                    insertableCols.add(colName);
//
//                    Object colVal = convertValue(val, dataType);
//                    try {
//                    	values.add(colVal);
//                    } catch (NumberFormatException e) {
//                        throw new RuntimeException("Invalid value for column: " + colName + " (" + dataType + ")");
//                    }
//                }
//            }
//
//            if (insertableCols.isEmpty()) {
//                return Map.of("status", "failed", "message", "No valid columns provided for insert");
//            }
//
//            // SQL query
//            String colNames = String.join(", ", insertableCols);
//            String placeholders = String.join(", ", Collections.nCopies(insertableCols.size(), "?"));
//
//            String sql = String.format("INSERT INTO %s.%s (%s) VALUES (%s)", schema, table, colNames, placeholders);
//
//            jdbcTemplate.update(sql, values.toArray());
//
//            return Map.of("status", "success", "message", "Row inserted successfully");
//
//   }

@PostMapping("/{schema}/{table}")
public Map<String, Object> insertRow(
        @PathVariable String schema,
        @PathVariable String table,
        @RequestBody Map<String, Object> rowData,
        HttpServletRequest request) throws SQLException {

    List<Map<String, Object>> columns = metadataService.getColumns(schema, table);
    List<String> pkColumns = metadataService.getPrimaryKeys(schema, table);

    // 1️⃣ Auto system fields
//    String currentUser = getCurrentUsername(); // JWT / SecurityContext se
    String currentUser = "System";
    String ipAddress = request.getRemoteAddr();
    LocalDateTime  now = LocalDateTime.now();

    // Common auto fields
    Map<String, Object> autoFields = new HashMap<>();
    autoFields.put("created_by",currentUser );
    autoFields.put("created_date", now);
    autoFields.put("created_ip_addr", IPUtil.getClientIp(request));
//    autoFields.put("created_mac_addr", NetworkUtils.generateRandomMacAddress());
    autoFields.putIfAbsent("status", "ACTIVE");

    // Merge frontend data + auto fields
    Map<String, Object> finalData = new HashMap<>(rowData);
    finalData.putAll(autoFields);

    // 2️⃣ Build insertable columns and values
    List<String> insertableCols = new ArrayList<>();
    List<Object> values = new ArrayList<>();

    for (Map<String, Object> col : columns) {
        String colName = (String) col.get("name");
        String dataType = (String) col.get("type");

        // Skip PK if auto-generated
        if (pkColumns.contains(colName)) continue;

        if (finalData.containsKey(colName)) {
            Object val = finalData.get(colName);
            if (val != null && !val.toString().trim().isEmpty()) {
                insertableCols.add(colName);
                values.add(convertValue(val, dataType));
            }
        }
    }

    if (insertableCols.isEmpty()) {
        return Map.of("status", "failed", "message", "No valid columns provided for insert");
    }

    // 3️⃣ Execute insert
    String colNames = String.join(", ", insertableCols);
    String placeholders = String.join(", ", Collections.nCopies(insertableCols.size(), "?"));
    String sql = String.format("INSERT INTO %s.%s (%s) VALUES (%s)", schema, table, colNames, placeholders);

    jdbcTemplate.update(sql, values.toArray());

    return Map.of("status", "success", "message", "Row inserted successfully", "data", finalData);
}

//
//    @PutMapping("/{schema}/{table}/{id}")
//    public int updateRow(
//            @PathVariable String schema,
//            @PathVariable String table,
//            @PathVariable String id,
//            @RequestBody Map<String, Object> rowData) throws SQLException {
//
//        // Get PKs
//        List<String> pkColumns = metadataService.getPrimaryKeys(schema, table);
//        if (pkColumns.isEmpty()) {
//            throw new RuntimeException("No primary key defined for " + schema + "." + table);
//        }
//        String pk = pkColumns.get(0);
//
//        // Get column metadata
//        List<Map<String, Object>> columns = metadataService.getColumns(schema, table);
//
//        // Build SET clause dynamically
//        List<String> updatableCols = new ArrayList<>();
//        List<Object> values = new ArrayList<>();
//
//        for (Map<String, Object> col : columns) {
//            String colName = (String) col.get("name");
//            String dataType = (String) col.get("type");
//
//            // Skip auto PKs
//            if (pkColumns.contains(colName)) {
//                continue;
//            }
//
//            if (rowData.containsKey(colName)) {
//                Object val = rowData.get(colName);
//
//                // Skip null/empty
//                if (val == null || val.toString().trim().isEmpty()) {
//                    continue;
//                }
//
//                // Add column to update list
//                updatableCols.add(colName + " = ?");
//
//                // Convert type properly (like insert)
//                Object colVal = convertValue(val, dataType);
//                values.add(colVal);
//            }
//        }
//
//        if (updatableCols.isEmpty()) {
//            throw new RuntimeException("No updatable columns provided for " + schema + "." + table);
//        }
//
//        // Build SQL
//        String setClause = String.join(", ", updatableCols);
//        String sql = "UPDATE " + schema + "." + table + " SET " + setClause + " WHERE " + pk + " = ?";
//
//        // Add PK value at end
//        values.add(convertValue(id, getColumnType(columns, pk)));
//
//        return jdbcTemplate.update(sql, values.toArray());
//    }

    @PutMapping("/{schema}/{table}/{id}")
    public int updateRow(
            @PathVariable String schema,
            @PathVariable String table,
            @PathVariable String id,
            @RequestBody Map<String, Object> rowData,
            HttpServletRequest request) throws SQLException {

        List<Map<String, Object>> columns = metadataService.getColumns(schema, table);
        List<String> pkColumns = metadataService.getPrimaryKeys(schema, table);

        if (pkColumns.isEmpty()) {
            throw new RuntimeException("No primary key defined for " + schema + "." + table);
        }
        String pk = pkColumns.get(0);

        // 1️⃣ Auto system fields
//        String currentUser = getCurrentUsername(); // JWT / SecurityContext se
        String currentUser = "System";
        String ipAddress = request.getRemoteAddr();
        LocalDateTime now = LocalDateTime.now();

        rowData.put("modified_by", currentUser);
        rowData.put("modified_date",  LocalDateTime.now());
        rowData.put("modified_ip_addr",  IPUtil.getClientIp(request));
//        rowData.put("modified_mac_addr", NetworkUtils.generateRandomMacAddress());


        // 2️⃣ Build SET clause
        List<String> updatableCols = new ArrayList<>();
        List<Object> values = new ArrayList<>();

        for (Map<String, Object> col : columns) {
            String colName = (String) col.get("name");
            String dataType = (String) col.get("type");

            // Skip PK
            if (pkColumns.contains(colName)) continue;

            if (rowData.containsKey(colName)) {
                Object val = rowData.get(colName);
                if (val != null && !val.toString().trim().isEmpty()) {
                    updatableCols.add(colName + " = ?");
                    values.add(convertValue(val, dataType));
                }
            }
        }

        if (updatableCols.isEmpty()) {
            throw new RuntimeException("No updatable columns provided for " + schema + "." + table);
        }

        // 3️⃣ Add PK value at end
        values.add(convertValue(id, getColumnType(columns, pk)));

        String setClause = String.join(", ", updatableCols);
        String sql = "UPDATE " + schema + "." + table + " SET " + setClause + " WHERE " + pk + " = ?";

        return jdbcTemplate.update(sql, values.toArray());
    }



    private String getColumnType(List<Map<String, Object>> columns, String columnName) {
        return columns.stream()
                .filter(c -> columnName.equalsIgnoreCase((String) c.get("name")))
                .map(c -> (String) c.get("type"))
                .findFirst()
                .orElse("text");
    }
    
    private Object convertValue(Object value, String dataType) {
        if (value == null) return null;
        String str = value.toString();

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
            case "timestamp":
            case "timestamptz":
            case "timestamp with time zone":
                try {
                    // First try normal ISO parsing
                    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");
                    String cleanedStr = str;

                    // Truncate nanoseconds if present
                    int dotIndex = str.indexOf(".");
                    if (dotIndex != -1) {
                        cleanedStr = str.substring(0, dotIndex);
                    }

                    LocalDateTime ldt = LocalDateTime.parse(cleanedStr, formatter);
                    return Timestamp.valueOf(ldt);
                } catch (DateTimeParseException e) {
                    // fallback: just try Timestamp.valueOf directly
                    return Timestamp.valueOf(str.replace("T", " "));
                }


            case "time":
            case "timetz":
            case "time with time zone":
                if (str.contains("+")) {
                    return Time.valueOf(str.split("\\+")[0]); // drop timezone offset
                } else {
                    return Time.valueOf(str);
                }
            default:
                return str; // fallback: varchar, text
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
    
 // Define formatter compatible with <input type="datetime-local">
//    private static final DateTimeFormatter HTML5_DATE_TIME =
//            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");

//    private Object normalizeValue(String type, Object value) {
//        if (value == null) return null;
//
//        try {
//            if (type != null && type.toLowerCase().contains("timestamp")) {
//                if (value instanceof Timestamp ts) {
//                    return ts.toLocalDateTime().format(HTML5_DATE_TIME);
//                }
//                if (value instanceof OffsetDateTime odt) {
//                    return odt.toLocalDateTime().format(HTML5_DATE_TIME);
//                }
//                if (value instanceof LocalDateTime ldt) {
//                    return ldt.format(HTML5_DATE_TIME);
//                }
//                // Last fallback: try parsing string
//                return LocalDateTime.parse(value.toString().replace("Z",""))
//                        .format(HTML5_DATE_TIME);
//            }
//        } catch (Exception e) {
//            // Fallback: return original
//            return value.toString();
//        }
//
//        return value;
//    }
    
//    @GetMapping("/{schema}/{table}/fk-values/{column}")
//    public List<Map<String, Object>> getForeignKeyValues(
//            @PathVariable String schema,
//            @PathVariable String table,
//            @PathVariable String column) throws SQLException {
//
//        // Get FK metadata
//    	//System.err.println("-----------------");
//        List<Map<String, Object>> fks = metadataService.getForeignKeys(schema, table);
//        Map<String, Object> fkInfo = fks.stream()
//                .filter(fk -> fk.get("fkColumn").equals(column))
//                .findFirst()
//                .orElseThrow(() -> new RuntimeException("No FK found for column " + column));
//
//        String pkTable = (String) fkInfo.get("pkTable");
//        String pkTableSchema = (String) fkInfo.get("pkTableSchema");
////        String pkColumn = (String) fkInfo.get("pkColumn");
////        String pkColumnValue = (String) fkInfo.get("displayColumn");
////        if(!pkColumnValue.isBlank() && !pkColumnValue.equalsIgnoreCase("NONE")) {
////        	pkColumnValue = ", "+pkColumnValue+" as value ";
////        }else {
////        	pkColumnValue = ", "+pkColumn+" as value ";
////        }
//
//        //String sql = "SELECT " + pkColumn + " as id FROM " + schema + "." + pkTable;
//        //String sql = "SELECT " + pkColumn + " as id"+pkColumnValue+" FROM " + pkTableSchema + "." + pkTable;
//        String dropdown = getDropdownColumnsForFerignKeys(pkTableSchema, pkTable);
//        String sql = "SELECT " + dropdown + " FROM " + pkTableSchema + "." + pkTable;
//        return jdbcTemplate.queryForList(sql);
//    }

//Exact

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
//@GetMapping("/{schema}/{table}/fk-values/{column}")
//public List<Map<String, Object>> getForeignKeyValues(
//        @PathVariable String schema,
//        @PathVariable String table,
//        @PathVariable String column) {
//
//    List<Map<String, Object>> fks = metadataService.getForeignKeys(schema, table);
//
//    // Filter by requested column
//    Map<String, Object> fkInfo = fks.stream()
//            .filter(fk -> fk.get("fkColumn").equals(column))
//            .findFirst()
//            .orElseThrow(() -> new RuntimeException("No FK found for column " + column));
//
//    String pkTable = (String) fkInfo.get("pkTable");
//    String pkTableSchema = (String) fkInfo.get("pkTableSchema");
//    String pkColumn = (String) fkInfo.get("pkColumn");
//    String displayColumn = (String) fkInfo.get("displayColumn");
//
//    String sql = "SELECT \"" + pkColumn + "\" as id, \"" + displayColumn + "\" as value FROM "
//            + pkTableSchema + "." + pkTable;
//
//    return jdbcTemplate.queryForList(sql);
//}
//




    public String getDropdownColumnsForForeignKeys(String schema, String table) {
    	try {
    		if(schema.isBlank() && table.isBlank()) {
    			return "";
    		}
    		if(getValidSchemaList(schema)) {
    			String tableName = schema+"."+table;
    			String key = getProperty(tableName+".key");
    			String val = getProperty(tableName+".val");

    			if(!key.isBlank() && !val.isBlank()) {
    				return key + " as id, "+val+" as value ";
    			}else {
    				//System.err.println("Key and Value unavailable for table ---"+tableName);
    				log.error("Key and Value unavailable for table ---"+tableName);
    			}
    		}else {
    			log.error("Invalid Schema -- "+schema);
    		}
    	}catch (Exception e) {
			//e.printStackTrace();
			log.error("Exception--->"+e);
		}
    	return "";
    }

    public boolean getValidSchemaList(String schema) {
    	try {
    		if(!schema.isBlank()) {
    			String schemas = getProperty("valid.schema.list");
    			List<String> schemaList = new ArrayList<>(Arrays.asList(schemas.split(",")));
    			if(schemaList.contains(schema)) {
    				return true;
    			}
    		}else {
    			return false;
    		}
    	}catch (Exception e) {
    		//e.printStackTrace();
    		log.error("Exception--->"+e);
		}
    	return false;
    }
    
    public String getProperty(String val) {
    	String propValue = "";
    	try {
    		Properties props = new Properties();
			props.load(new ClassPathResource("tables-dropdown.properties").getInputStream());
			propValue = props.getProperty(val);
			if(!propValue.isBlank()) {
				return propValue;
			}
    	}catch (Exception e) {
    		//e.printStackTrace();
    		log.error("Exception--->"+e);
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

//    @GetMapping("/schemas")
//    public List<String> getSchemas() {
//        return metadataService.getAllSchemas();
//    }

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

//    @GetMapping("/tables/{schema}")
//    public ResponseEntity<List<String>> getTables(@PathVariable String schema) {
//        // Validate schema
//        if (!Arrays.asList("mst", "adm").contains(schema.toLowerCase())) {
//            return ResponseEntity.badRequest()
//                    .body(Collections.emptyList());
//        }
//
//        List<String> tables = metadataService.getTablesBySchema(schema.toLowerCase());
//        return ResponseEntity.ok(tables);
//    }

}
