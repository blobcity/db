/**
 * Copyright (C) 2018 BlobCity Inc
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published
 * by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.blobcity.db.schema;

import com.blobcity.db.exceptions.ErrorCode;
import com.blobcity.db.exceptions.OperationException;
import com.blobcity.db.lang.columntypes.FieldType;
import com.blobcity.db.lang.columntypes.FieldTypeFactory;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author sanketsarang
 */
public class SchemaBuilder {
    private TableType tableType;
    private ReplicationType replicationType;
    private int replicationFactor;
    private boolean flexibleSchema;
    private Map<String, Column> columnMap = new HashMap<>();
    private String primaryKeyColumName;
    private String dataFileForInMemoryTableSchema; // use this file name to find the columns for in memory table
    private Boolean dataFileHasHeader; // if the first row contains names of columns, default false
    private Integer dataFileLinesToSkip; // lines to skip, some data files start with comments, default 0
    private String dataFileType;
    private String dataFileSeparator;
    
    Logger logger = LoggerFactory.getLogger(SchemaBuilder.class);
    
    public SchemaBuilder() {
        //do nothing
    }
    
    public SchemaBuilder(final TableType tableType, final ReplicationType replicationType, final int replicationFactor, final boolean flexibleSchema) {
        this.tableType = tableType;
        this.replicationType = replicationType;
        this.replicationFactor = replicationFactor;
        this.flexibleSchema = flexibleSchema;
        
        this.dataFileForInMemoryTableSchema = null;
        this.dataFileHasHeader = false;
        this.dataFileLinesToSkip = 0;
        this.dataFileType = null;
        this.dataFileSeparator = null;
    }
    
    public SchemaBuilder(final TableType tableType, final ReplicationType replicationType, final int replicationFactor, final boolean flexibleSchema,
            final String dataFileForInMemoryTableSchema, final Boolean dataFileHasHeader, final Integer dataFileLinesToSkip,
            final String dataFileType, final String dataFileSeparator) {
        this.tableType = tableType;
        this.replicationType = replicationType;
        this.replicationFactor = replicationFactor;
        this.flexibleSchema = flexibleSchema;
        
        this.dataFileForInMemoryTableSchema = dataFileForInMemoryTableSchema;
        this.dataFileHasHeader = dataFileHasHeader;
        this.dataFileLinesToSkip = dataFileLinesToSkip;
        this.dataFileType = dataFileType;
        this.dataFileSeparator = dataFileSeparator;
    }

    public static SchemaBuilder newDefault() {
        return new SchemaBuilder(TableType.ON_DISK, ReplicationType.DISTRIBUTED, 0, true);
    }
    
    public static SchemaBuilder newInstance() {
        return new SchemaBuilder();
    }
    
    public SchemaBuilder type(TableType tableType) { 
        this.tableType = tableType;
        return this;
    }
    
    public SchemaBuilder replication(ReplicationType replicationType, int replicationFactor) {
        this.replicationType = replicationType;
        this.replicationFactor = replicationFactor;
        return this;
    }
    
    public SchemaBuilder replication(ReplicationType replicationType) {
        this.replicationType = replicationType;
        return this;
    }
    
    public SchemaBuilder replication(int replicationFactor) {
        this.replicationFactor = replicationFactor;
        return this;
    }
    
    public SchemaBuilder withFlexibleSchema() {
        this.flexibleSchema = true;
        return this;
    }
    
    public SchemaBuilder withoutFlexibleSchema() {
        this.flexibleSchema = false;
        return this;
    }
    
    public SchemaBuilder flexibleSchema(boolean flexibleSchema) {
        this.flexibleSchema = flexibleSchema;
        return this;
    }
    
    public SchemaBuilder addColumn(final Column column) {
        this.columnMap.put(column.getName(), column);
        return this;
    }
    
    public SchemaBuilder withPrimaryKey(final String primaryKeyColumnName) {
        this.primaryKeyColumName = primaryKeyColumnName;
        return this;
    }
    
    public SchemaBuilder dataFile(final String dataFileForInMemoryTableSchema) {
        this.dataFileForInMemoryTableSchema = dataFileForInMemoryTableSchema;
        return this;
    }
    
    public SchemaBuilder dataFileLinesToSkip(final Integer dataFileLinesToSkip) {
        this.dataFileLinesToSkip = dataFileLinesToSkip;
        return this;
    }
    
    public SchemaBuilder dataFileType(final String dataFileType) {
        this.dataFileType = dataFileType;
        return this;
    }
    
    public SchemaBuilder dataFileSeparator(final String dataFileSeparator) {
        this.dataFileSeparator = dataFileSeparator;
        return this;
    }
    
    public SchemaBuilder dataFileHasHeader(final Boolean dataFileHasHeader) {
        this.dataFileHasHeader = dataFileHasHeader;
        return this;
    }
    
    public SchemaBuilder inMemoryParameters(final String dataFile, final Integer dataFileLinesToSkip,
            final String dataFileType, final String dataFileSeparator, final Boolean dataFileHasHeader) {
        this.dataFileForInMemoryTableSchema = dataFileForInMemoryTableSchema;
        this.dataFileLinesToSkip = dataFileLinesToSkip;
        this.dataFileType = dataFileType;
        this.dataFileSeparator = dataFileSeparator;
        this.dataFileHasHeader = dataFileHasHeader;
        return this;
        
    }
    
    public Object interpret(Object val) throws IllegalArgumentException {
        String s = val.toString();
        Scanner sc = new Scanner(s);

        return sc.hasNextInt() ? sc.nextInt()
                : sc.hasNextLong() ? sc.nextLong()
                : sc.hasNextDouble() ? sc.nextDouble()
                : sc.hasNextBoolean() ? sc.nextBoolean()
                : sc.hasNextBigInteger() ? sc.nextBigInteger()
                : sc.hasNextFloat() ? sc.nextFloat()
                : sc.hasNextByte() ? sc.nextByte()
                : sc.hasNext() ? sc.next()
                : s;
    }
    
    public JSONObject toJson() throws OperationException, FileNotFoundException, IOException {
        String[] colNames = {};
        String[] colValues = {};
        
        JSONObject schemaJson = new JSONObject();
        JSONObject metaJson = new JSONObject();
        
         /* Create meta json */
        metaJson.put(SchemaProperties.REPLICATION_TYPE, replicationType.getType());
        metaJson.put(SchemaProperties.REPLICATION_FACTOR, replicationFactor);
        metaJson.put(SchemaProperties.TABLE_TYPE, tableType.getType());
        metaJson.put(SchemaProperties.FLEXIBLE_SCHEMA, flexibleSchema);
        schemaJson.put(SchemaProperties.META, metaJson);
         
        
        if (tableType.equals(TableType.IN_MEMORY)) {
            IndexTypes indexType = IndexTypes.fromString("unique");
            AutoDefineTypes autoDefineType = AutoDefineTypes.fromString("uuid");
            FieldType fieldType = FieldTypeFactory.fromString("string");
            if(primaryKeyColumName == null) primaryKeyColumName = SchemaProperties.PRIMARY_KEY_COL_NAME;
            Column column = new Column(primaryKeyColumName, fieldType, indexType, autoDefineType);
            columnMap.put(primaryKeyColumName, column);
            
            logger.debug("dataFile: " +dataFileForInMemoryTableSchema);
            logger.debug("dataFileLinesToSkip: " +dataFileLinesToSkip);
            logger.debug("dataFileType: " +dataFileType);
            logger.debug("dataFileSeparator: " +dataFileSeparator);
            logger.debug("dataFileHasHeader: " +dataFileHasHeader);
            
            if (dataFileForInMemoryTableSchema == null) {
                return schemaJson;
            }
            BufferedReader reader = new BufferedReader(new FileReader(dataFileForInMemoryTableSchema));
            for (int j = 0; j < dataFileLinesToSkip; j++) {
                reader.readLine();
            }
            String cols = null;
            if (dataFileHasHeader) {
                cols = reader.readLine();

                String values = reader.readLine();
                if (dataFileSeparator.equals("space")) {
                    colNames = cols.split("\\s+");
                    colValues = values.split("\\s+");
                } else if (dataFileSeparator.equals("comma")) {
                    colNames = cols.split("\\s*,\\s*");
                    colValues = values.split("\\s*,\\s*");
                } else if (dataFileSeparator.equals("tab")) {
                    colNames = cols.split("\\t+");
                    colValues = values.split("\\t+");
                }
            } else {
                String values = reader.readLine();
                if (dataFileSeparator.equals("space")) {
                    colValues = values.split("\\s+");
                } else if (dataFileSeparator.equals("comma")) {
                    colValues = values.split("\\s*,\\s*");
                } else if (dataFileSeparator.equals("tab")) {
                    colValues = values.split("\\t+");
                }
                for (Integer j = 0; j < colValues.length; j++) {
                    Integer k = j + 1;
                    colNames[j] = "col" + k.toString();
                }
            }
            
            for (int i = colNames.length - 1; i >= 0; i--) {
                String currentColName = colNames[i];
                String currentColValue = colValues[i];
                if (!columnMap.containsKey(currentColName)) {
                    indexType = IndexTypes.fromString("none");
                    autoDefineType = AutoDefineTypes.fromString("none");
                    Object o = interpret(currentColValue);
                    if (o.toString().contains("E") || o.toString().contains("e")) { // exponent
                        fieldType = FieldTypeFactory.fromString("BigDecimal");
                    } else {
                        fieldType = FieldTypeFactory.fromString(o.getClass().getSimpleName());
                    }
                    Column columnn = new Column(currentColName, fieldType, indexType, autoDefineType);
                    columnMap.put(currentColName, columnn);
                }
            }
        }

        schemaJson.put(SchemaProperties.PRIMARY, primaryKeyColumName);

        if(columnMap.isEmpty()) {
            throw new OperationException(ErrorCode.INVALID_SCHEMA);
        }
        
        /* Create columns json */
        JSONObject colsJson = new JSONObject();
//        colsJson.put(SchemaProperties.PRIMARY, primaryKeyColumName);
        for (String columnName : columnMap.keySet()) {
            Column columnn = columnMap.get(columnName);
            JSONObject columnJson = new JSONObject();
            columnJson.put(SchemaProperties.COLS_NAME, columnn.getName());
            columnJson.put(SchemaProperties.COLS_DATA_TYPE, columnn.getFieldType().toJson());
            columnJson.put(SchemaProperties.COLS_AUTO_DEFINE, columnn.getAutoDefineType().getText());
            columnJson.put(SchemaProperties.COLS_INDEX, columnn.getIndexType().getText());
            colsJson.put(columnn.getName(), columnJson);
        }
        
        schemaJson.put(SchemaProperties.COLS, colsJson);
        
        return schemaJson;
    }
}
