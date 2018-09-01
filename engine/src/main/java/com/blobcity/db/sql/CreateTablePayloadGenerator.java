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

package com.blobcity.db.sql;

import com.blobcity.db.exceptions.ErrorCode;
import com.blobcity.db.exceptions.OperationException;
import com.blobcity.db.lang.columntypes.FieldType;
import com.blobcity.db.lang.columntypes.StringField;
import com.blobcity.db.schema.*;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.json.JSONObject;

/**
 * Used to parse an SQL CREATE TABLE statement and generate a JSON payload.
 *
 * @author akshaydewan
 * @author sanketsarang
 */
public class CreateTablePayloadGenerator {

    private final Map<String, JSONObject> columnJsonMap; //TODO: Remove this and all related dependencies
    private final Map<String, Column> columnMap;
    private String primaryKeyColumn = null;
    private ReplicationType replicationType = ReplicationType.DISTRIBUTED;
    private int replicationFactor = 0;
    private boolean flexibleSchema = false;
    private TableType tableType = TableType.ON_DISK;

    public CreateTablePayloadGenerator() {
        columnJsonMap = new HashMap<>();
        columnMap = new HashMap<>();
    }

    private JSONObject fetchColumnDef(String columnName) {
        JSONObject columnJSON;
        if (columnJsonMap.containsKey(columnName)) {
            columnJSON = columnJsonMap.get(columnName);
        } else {
            columnJSON = new JSONObject();
        }
        return columnJSON;
    }
    
    private Column fetchColumn(String columnName) {
        Column column;
        if (columnMap.containsKey(columnName)) {
            column = columnMap.get(columnName);
        } else {
            column = new Column();
            column.setAutoDefineType(AutoDefineTypes.NONE);
            column.setIndexType(IndexTypes.NONE);
        }
        return column;
    }

    /**
     * Add a column definition to the payload. 
     * 
     * TODO: Add support for data type constraints
     *
     * @param columnName The name of the column
     * @param fieldType The field type of the column
     */
    public void putColumn(String columnName, FieldType fieldType) {
        Preconditions.checkNotNull(columnName);
        Preconditions.checkNotNull(fieldType);
        
        JSONObject columnJSON = fetchColumnDef(columnName);
        columnJSON.put("type", fieldType.toJson());
        columnJsonMap.put(columnName, columnJSON);
        
        Column column = fetchColumn(columnName);
        column.setName(columnName);
        column.setFieldType(fieldType);
        columnMap.put(columnName, column);
    }

    /**
     * Sets a single column as a primary key
     *
     * @param columnName A valid column name
     */
    public void setPrimaryKey(String columnName) {
        if (columnName == null) {
            throw new IllegalArgumentException("columnName must not be null");
        }
        primaryKeyColumn = columnName;
        addUniqueConstraint(columnName);
        addUuidAutoDefine(columnName);
    }

    /**
     * Adds a UNIQUE constraint for a single column
     *
     * @param columnName
     */
    public void addUniqueConstraint(String columnName) {
        Preconditions.checkNotNull(columnName);
        
        JSONObject columnJSON = fetchColumnDef(columnName);
        columnJSON.put("index", IndexTypes.UNIQUE.getText());
        columnJsonMap.put(columnName, columnJSON);
        
        Column column = fetchColumn(columnName);
        column.setIndexType(IndexTypes.UNIQUE);
        columnMap.put(columnName, column);
    }

    /**
     * Sets the column to auto-defined uuid
     * @param columnName name of column
     */
    public void addUuidAutoDefine(String columnName) {
        Preconditions.checkNotNull(columnName);

        JSONObject columnJSON = fetchColumnDef(columnName);
        columnJSON.put("auto-define", AutoDefineTypes.UUID.getText());
        columnJsonMap.put(columnName, columnJSON);

        Column column = fetchColumn(columnName);
        try {
            column.setFieldType(new StringField(Types.STRING));
            column.setAutoDefineType(AutoDefineTypes.UUID);
            columnMap.put(columnName, column);
        } catch (OperationException e) {
            e.printStackTrace();
        }
    }
    
    public void addReplicationSettings(ReplicationType replicationType, int replicationFactor) {
        this.replicationType = replicationType;
        this.replicationFactor = replicationFactor;
    }
    
    public void enableFlexibleSchema() {
        this.flexibleSchema = true;
    }
    
    public void disableFlexibleSchema() {
        this.flexibleSchema = false;
    }
    
    public void setTableType(TableType tableType) {
        this.tableType = tableType;
    }

    /**
     * Generates a JSON payload for table creation
     *
     * @return A JSONObject containing the payload for table generation
     * @throws OperationException If the table specifications are not supported by BlobCity
     */
    public JSONObject generate() throws OperationException, IOException {
        if (StringUtils.isEmpty(primaryKeyColumn)) {
            throw new OperationException(ErrorCode.OPERATION_NOT_SUPPORTED, "Cannot create a table without a primary key");
        }

        SchemaBuilder builder = SchemaBuilder.newDefault()
                .type(tableType)
                .flexibleSchema(flexibleSchema)
                .replication(replicationType, replicationFactor)
                .withPrimaryKey(primaryKeyColumn);
        for(Map.Entry<String, Column> entry : columnMap.entrySet()) {
            builder.addColumn(entry.getValue());
        }
        
        builder.withPrimaryKey(primaryKeyColumn);

        JSONObject responseJson = builder.toJson();

        return responseJson;
    }
}
