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

package com.blobcity.db.schema.beans;

import com.blobcity.db.exceptions.ErrorCode;
import com.blobcity.db.exceptions.OperationException;
import com.blobcity.db.lang.columntypes.FieldTypeFactory;
import com.blobcity.db.schema.AutoDefineTypes;
import com.blobcity.db.schema.Column;
import com.blobcity.db.schema.ColumnMapping;
import com.blobcity.db.schema.Types;
import com.blobcity.db.schema.IndexTypes;
import com.blobcity.db.schema.Schema;
import com.blobcity.db.sql.util.PathUtil;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * This class manages the schema of tables created
 * 
 * @author sanketsarang
 */
@Component
public class SchemaManager {

    private static final Logger logger = LoggerFactory.getLogger(SchemaManager.class.getName());

    public Schema readSchema(final String appId, final String table) throws OperationException {
        final String schemaJsonString = readSchemaFile(appId, table);
        try {
            return new Schema(new JSONObject(schemaJsonString));
        } catch (JSONException ex) {
            logger.error(null, ex);
            throw new OperationException(ErrorCode.SCHEMA_FILE_READ_FAILED, "Unable to read schema file for table " + table + " inside app " + appId);
        }
    }

    public void writeSchema(final String appId, final String table, final Schema schema) throws OperationException, JSONException {
        ensureSchemaValid(schema);
        writeSchema(appId, table, schema.toJSONString(), false);
        syncColumnMapping(appId, table, schema);
    }

    public void writeSchema(final String appId, final String table, final Schema schema, final boolean force) throws OperationException, JSONException {
        ensureSchemaValid(schema);
        writeSchema(appId, table, schema.toJSONString(), force);
        syncColumnMapping(appId, table, schema);
    }

    public ColumnMapping readColumnMapping(final String appId, final String table) throws OperationException {
        try {
            return new ColumnMapping(new JSONObject(readColumnMappingFile(appId, table)));
        } catch (FileNotFoundException ex) {
            throw new OperationException(ErrorCode.SCHEMA_FILE_NOT_FOUND, "Internal column mapping schema file could not be found");
        } catch (JSONException ex) {
            logger.error(null, ex);
            throw new OperationException(ErrorCode.SCHEMA_CORRUPTED, "Internal column mapping schema file seems to be corrupted");
        }
    }

    public void writeColumnMapping(final String appId, final String table, final ColumnMapping columnMapping) throws OperationException {
        try {
            writeColumnMapping(appId, table, columnMapping.toJSONString());
        } catch (JSONException ex) {
            logger.error(null, ex);
            throw new OperationException(ErrorCode.SCHEMA_CORRUPTED, "Internal column mapping schema seems to be corrupted");
        }
    }

    /**
     * Converts internal key names to viewable key names. Data is automatically normalized. Columns no longer existent
     * in the schema are ignored and columns in schema that are not currently present in the viewable JSON passed are
     * added as blank values.
     *
     * @param appId
     * @param table
     * @param jsonObject
     * @return
     * @throws OperationException
     */
    public JSONObject internalToViewable(final String appId, final String table, JSONObject jsonObject) throws OperationException {
        JSONObject viewableJson = new JSONObject();
        Schema schema = SchemaStore.getInstance().getSchema(appId, table);
        ColumnMapping mapping = SchemaStore.getInstance().getColumnMapping(appId, table);

        schema.getColumnMap().values().stream().forEach((column) -> {
            try {
                if (jsonObject.has(mapping.getInternalName(column.getName()))) {
                    viewableJson.put(column.getName(), jsonObject.get(mapping.getInternalName(column.getName())));
                }
            } catch (JSONException ex) {
                logger.error(null, ex);
            }
        });

        return viewableJson;
    }

    public JSONObject viewableToInternal(final String appId, final String table, JSONObject jsonObject) throws OperationException {
        JSONObject internalJson = new JSONObject();
        Schema schema = SchemaStore.getInstance().getSchema(appId, table);
        ColumnMapping mapping = SchemaStore.getInstance().getColumnMapping(appId, table);

        for (Column column : schema.getColumnMap().values()) {
            try {
                if (jsonObject.has(column.getName())) {
                    internalJson.put(mapping.getInternalName(column.getName()), jsonObject.get(column.getName()));
                }
            } catch (JSONException ex) {
                logger.error(null, ex);
            }
        }

        return internalJson;
    }

    private void ensureSchemaValid(final Schema schema) throws OperationException {

        if (schema.getTableType() == null) {
            throw new OperationException(ErrorCode.INVALID_SCHEMA, "Table type must be specified");
        }

        if (schema.getReplicationType() == null) {
            throw new OperationException(ErrorCode.INVALID_SCHEMA, "Replication type must be specified");
        }

        switch (schema.getReplicationType()) {
            case DISTRIBUTED:
                if (schema.getReplicationFactor() < 0) {
                    throw new OperationException(ErrorCode.INVALID_SCHEMA, "Repliciation factor must have a value greater than 0. Found value: " + schema.getReplicationFactor());
                }
                break;
        }

        /* Short circuit this function if no columns are specified */
        if (schema.getColumnMap().isEmpty()) {
            return;
        }

        /* Check if column marked as primary is present */
        if (!schema.getColumnMap().containsKey(schema.getPrimary())) {
            throw new OperationException(ErrorCode.INVALID_SCHEMA, "No column specification found for primary key: " + schema.getPrimary());
        }

        /* Check that primary key column is of permitted type */
        switch (schema.getColumnMap().get(schema.getPrimary()).getFieldType().getType()) {
            case CHAR:
            case CHARACTER:
            case CHARACTER_VARYING:
            case CHAR_VARYING:
            case VARCHAR:
            case NATIONAL_CHAR:
            case NATIONAL_CHARACTER:
            case NCHAR:
            case NATIONAL_CHARACTER_VARYING:
            case NATIONAL_CHAR_VARYING:
            case NCHAR_VARYING:
            case STRING:
            case NUMERIC:
            case DECIMAL:
            case DEC:
            case SMALLINT:
            case INTEGER:
            case INT:
            case BIGINT:
            case LONG:
            case REAL:
            case DOUBLE:
            case DOUBLE_PRECISION:
            case FLOAT:
            case BOOLEAN:
            case DATE:
            case TIME:
            case TIMESTAMP:
                break;
            default:
                throw new OperationException(ErrorCode.INVALID_SCHEMA, "primary key cannot be of type: " + schema.getColumnMap().get(schema.getPrimary()).getFieldType().getType().name());
        }

        /* Check that primary key has indexing option set to unique */
        if (schema.getColumnMap().get(schema.getPrimary()).getIndexType() != IndexTypes.UNIQUE) {
            throw new OperationException(ErrorCode.INVALID_SCHEMA, "primary key must have indexing type: unique");
        }

        for (String columnName : schema.getColumnMap().keySet()) {
            Column column = schema.getColumnMap().get(columnName);

            /* Check that all columns with UUID based auto-define type are of type string 
             * and all timestamp auto-define columns are of type timestamp
             */
            if (column.getAutoDefineType() == AutoDefineTypes.UUID && column.getFieldType().getType() != Types.STRING) {
                throw new OperationException(ErrorCode.INVALID_SCHEMA, "Column " + columnName + " must be of type String. UUID based auto-defined columns can only be of String type");
            } else if (column.getAutoDefineType() == AutoDefineTypes.TIMESTAMP && column.getFieldType().getType() != Types.TIMESTAMP) {
                throw new OperationException(ErrorCode.INVALID_SCHEMA, "Column " + columnName + " must be of type TIMESTAMP.");
            }

            /* Check that collection type columns have indexing & auto-defined type as none */
            switch (column.getFieldType().getType()) {
                case LIST_INTEGER:
                case LIST_FLOAT:
                case LIST_DOUBLE:
                case LIST_STRING:
                case LIST_LONG:
                case LIST_CHARACTER:
                case ARRAY:
                case MULTISET:
                    if (column.getIndexType() != IndexTypes.NONE) {
                        throw new OperationException(ErrorCode.INVALID_SCHEMA, "List cannot be indexed in column: " + columnName);
                    }

                    if (column.getAutoDefineType() != AutoDefineTypes.NONE) {
                        throw new OperationException(ErrorCode.INVALID_SCHEMA, "List cannot be auto-defined in column: " + columnName);
                    }
            }

            /* Restrict BITMAP indexes as they are not yet supported */
            switch (column.getIndexType()) {
                case BITMAP:
                    throw new OperationException(ErrorCode.OPERATION_NOT_SUPPORTED, "BITMAP indexes are not yet supported.");
            }
        }
    }

    /**
     * Reads schema in JSON format for the target table
     *
     * @param appId
     * @param table
     * @return JSON String representing schema of target table
     */
    private String readSchemaFile(final String appId, final String table) throws OperationException {
        try (BufferedReader reader = new BufferedReader(new FileReader(PathUtil.schemaFilePath(appId, table)))) {
            return reader.readLine();
        } catch (FileNotFoundException ex) {
            throw new OperationException(ErrorCode.SCHEMA_FILE_NOT_FOUND, "No schema file found for " + appId + "." + table);
        } catch (IOException ex) {
            throw new OperationException(ErrorCode.SCHEMA_FILE_READ_FAILED, "Schema file read failed for " + appId + "." + table);
        }
    }

    /**
     * Writes schema of the target table. This function simply replaces current schema with new schema. A flag is
     * provided to indicate whether the function is permitted to replace schema of an existing table with new values.
     * The function does NOT validate the schema, so pre-validation by external means is necessary
     *
     * @param appId
     * @param table
     * @param schemaJson
     * @param force indicates whether the function is allowed to perform write operation to replace an already existent
     * schema
     */
    private void writeSchema(final String appId, final String table, final String schemaJson, final boolean force) throws OperationException {
        String schemaFilePath = PathUtil.schemaFilePath(appId, table);

        /* Restrict schema change if schema file is already found and force option is not enabled */
        if (!force) {
            if (new File(schemaFilePath).exists()) {
                throw new OperationException(ErrorCode.SCHEMA_CHANGE_NOT_PERMITTED);
            }
        }

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(schemaFilePath))) {
            writer.write(schemaJson);
        } catch (FileNotFoundException ex) {
            throw new OperationException(ErrorCode.SCHEMA_FILE_NOT_FOUND);
        } catch (IOException ex) {

            //TODO: Notify admin
            throw new OperationException(ErrorCode.SCHEMA_FILE_READ_FAILED);
        } finally {
            SchemaStore.getInstance().invalidateSchema(appId, table);
        }
    }

    private String readColumnMappingFile(final String appId, final String table) throws OperationException, FileNotFoundException {

        if (!new File(PathUtil.columnMappingFilePath(appId, table)).exists()) {
            throw new FileNotFoundException();
        }

        try (BufferedReader reader = new BufferedReader(new FileReader(PathUtil.columnMappingFilePath(appId, table)))) {
            return reader.readLine();
        } catch (IOException ex) {
            logger.error(null, ex);
            throw new OperationException(ErrorCode.SCHEMA_CORRUPTED, "Please try to resync schema to perform auto-correction");
        }
    }

    private void writeColumnMapping(final String appId, final String table, final String json) throws OperationException {
        String schemaFilePath = PathUtil.columnMappingFilePath(appId, table);

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(schemaFilePath))) {
            writer.write(json);
        } catch (IOException ex) {
            logger.error(null, ex);
            throw new OperationException(ErrorCode.SCHEMA_CORRUPTED, "Please repeat operation after sometime to attempt auto correction");
        }
    }

    private void syncColumnMapping(final String appId, final String table, final Schema schema) throws JSONException, OperationException {
        boolean changed = false;
        Set<String> schemaColumnSet;
        Set<String> mappingColumnSet;

        JSONObject mappingJson;
        ColumnMapping columnMapping;
        try {
            mappingJson = new JSONObject(readColumnMappingFile(appId, table));
            columnMapping = new ColumnMapping(mappingJson);
        } catch (FileNotFoundException ex) {
            columnMapping = new ColumnMapping();
        }

        schemaColumnSet = schema.getColumnMap().keySet();
        mappingColumnSet = columnMapping.getViewableNameMap().keySet();

        /* The list of column names currently in schema but not in mapping. Add mapping for each such item */
        for (String columnName : schemaColumnSet) {
            if (mappingColumnSet.contains(columnName)) {
                continue;
            }
            columnMapping.addMapping(columnName);
            changed = true;
        }

        /* The list of column names currently in mapping but not in schema. Remove mapping for each such item */
        schemaColumnSet = schema.getColumnMap().keySet();
        Set<String> mappingColumnSetClone = new HashSet<>(mappingColumnSet);
        for (String columnName : mappingColumnSetClone) {
            if (schemaColumnSet.contains(columnName)) {
                continue;
            }
            columnMapping.removeMapping(columnName);
            changed = true;
        }

        if (changed) {
            writeColumnMapping(appId, table, columnMapping.toJSONString());
        }
    }

    public void insertPrimaryColumn(final String appId, final String table) throws OperationException{
        Schema schema;
        try {
            schema = readSchema(appId, table);

        } catch (OperationException ex) {
            if(ex.getErrorCode() == ErrorCode.SCHEMA_FILE_NOT_FOUND){
                schema = new Schema();
            }
            else throw ex;
        }

        schema.addDefaultPrimaryColumn();
        writeSchema(appId, table, schema, true);

        logger.info("New schema with pk looks like: " + schema.toJSONString());

//        Column column = new Column("_id", FieldTypeFactory.fromString("string"), IndexTypes.UNIQUE, AutoDefineTypes.UUID);
//        schema.getColumnMap().put("_id", column);
//        schema.setPrimary("_id");
//        writeSchema(appId, table, schema, true);
//        logger.info("New schema with pk looks like: " + schema.toJSONString());
    }

}
