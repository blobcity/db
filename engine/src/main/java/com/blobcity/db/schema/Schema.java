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
import java.io.Serializable;
import java.util.*;

import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author sanketsarang
 */
public class Schema implements Serializable {

    private static final long serialVersionUID = -7837144137104819623L;

    private String primary; //name of primary column
    private boolean indexingNeeded; //a flag indicating if any of the column in this schema requires indexing
    private Map<String, Column> map = new HashMap<>(); //column name v/s column object mapping
    private ReplicationType replicationType;
    private int replicationFactor; //valid only for ReplicationType.DISTRIBUTED
    private TableType tableType;
    private boolean flexibleSchema;

    public Schema() {
        primary = "";
        map = new HashMap<>();
        replicationType = ReplicationType.DISTRIBUTED; //default
        replicationFactor = 1; //default
        tableType = TableType.ON_DISK; //default
        flexibleSchema = true; //default
    }

    public boolean isIndexingNeeded() {
        return indexingNeeded;
    }

    public Schema(final JSONObject jsonObject) throws JSONException, OperationException {
        if (jsonObject == null) {
            throw new OperationException(ErrorCode.INVALID_SCHEMA, "Attempting to create schema with a null object");
        }

        /* Process meta */
        if(!jsonObject.has(SchemaProperties.META)) {
            throw new OperationException(ErrorCode.INVALID_SCHEMA, "No meta properties found within schema");
        }

        final JSONObject metaJson = jsonObject.getJSONObject(SchemaProperties.META);

        if (metaJson.has(SchemaProperties.REPLICATION_TYPE)) {
            replicationType = ReplicationType.fromString(metaJson.getString(SchemaProperties.REPLICATION_TYPE));
        } else {
            replicationType = ReplicationType.DISTRIBUTED;
        }

        if (replicationType == ReplicationType.DISTRIBUTED && metaJson.has(SchemaProperties.REPLICATION_FACTOR)) {
            replicationFactor = metaJson.getInt(SchemaProperties.REPLICATION_FACTOR);
        } else {
            replicationFactor = 0;
        }

        if (metaJson.has(SchemaProperties.TABLE_TYPE)) {
            tableType = TableType.fromString(metaJson.getString(SchemaProperties.TABLE_TYPE));
        } else {
            tableType = TableType.ON_DISK;
        }

        if (metaJson.has(SchemaProperties.FLEXIBLE_SCHEMA)) {
            flexibleSchema = metaJson.getBoolean(SchemaProperties.FLEXIBLE_SCHEMA);
        } else {
            flexibleSchema = true;
        }

        /* Process primary key */
        if (jsonObject.has(SchemaProperties.PRIMARY)) {
            primary = jsonObject.getString(SchemaProperties.PRIMARY);
        } else {
            primary = null;
        }

        /* Return if no columns present */
        if (!jsonObject.has(SchemaProperties.COLS)) {
            return;
        }

        final JSONObject colsJson = jsonObject.getJSONObject(SchemaProperties.COLS);

        Iterator<String> iterator = colsJson.keys();
        while (iterator.hasNext()) {
            final String key = iterator.next();

            if (key.equals(SchemaProperties.PRIMARY)) {
                continue;
            }

            JSONObject columnJson = colsJson.getJSONObject(key);
            final JSONObject typeJson = columnJson.getJSONObject("type");

            String autoDefine;
            try {
                autoDefine = columnJson.getString("auto-define");
            } catch (JSONException ex) {
                autoDefine = AutoDefineTypes.NONE.toString();
            } catch (Exception ex) {
                throw new OperationException(ErrorCode.INVALID_SCHEMA, "Invalid auto-define type");
            }

            String indexType;
            try {
                indexType = columnJson.getString("index");
                indexingNeeded = true;
            } catch (JSONException ex) {
                indexType = IndexTypes.NONE.toString();
            } catch (Exception ex) {
                throw new OperationException(ErrorCode.INVALID_SCHEMA, "Invalid index type");
            }

            Column column = new Column(key, FieldTypeFactory.fromJson(typeJson), IndexTypes.fromString(indexType), AutoDefineTypes.fromString(autoDefine));
            map.put(key, column);
        }
    }

    public String getPrimary() {
        return primary;
    }

    public void setPrimary(String primary) {
        this.primary = primary;
    }

    public Column getColumn(final String columnName) {
        return map.get(columnName);
    }

    public Map<String, Column> getColumnMap() {
        return map;
    }

    public void setColumnMap(Map<String, Column> columnList) {
        this.map = columnList;
    }

    public ReplicationType getReplicationType() {
        return replicationType;
    }

    public void setReplicationType(ReplicationType replicationType) {
        this.replicationType = replicationType;
    }

    public int getReplicationFactor() {
        return replicationFactor;
    }

    public void setReplicationFactor(int replicationFactor) {
        this.replicationFactor = replicationFactor;
    }

    public TableType getTableType() {
        return tableType;
    }

    public void setTableType(TableType tableType) {
        this.tableType = tableType;
    }

    public boolean isFlexibleSchema() {
        return flexibleSchema;
    }

    public void setFlexibleSchema(boolean flexibleSchema) {
        this.flexibleSchema = flexibleSchema;
    }
    
    public JSONObject toJSONObject() throws JSONException, OperationException {
        JSONObject metaJson = new JSONObject();
        JSONObject colsJson = new JSONObject();
        JSONObject jsonObject = new JSONObject();

        /* Create meta json */
        metaJson.put(SchemaProperties.REPLICATION_TYPE, replicationType.getType());
        metaJson.put(SchemaProperties.REPLICATION_FACTOR, replicationFactor);
        metaJson.put(SchemaProperties.TABLE_TYPE, tableType.getType());
        metaJson.put(SchemaProperties.FLEXIBLE_SCHEMA, flexibleSchema);

        jsonObject.put(SchemaProperties.META, metaJson);
        jsonObject.put(SchemaProperties.PRIMARY, primary);
        
        /* should at-least have the primary key column */
        if(map.isEmpty()) {
            return jsonObject;
        }

        /* Create columns json */
        for (String columnName : map.keySet()) {
            Column column = map.get(columnName);
            JSONObject columnJson = new JSONObject();
            columnJson.put(SchemaProperties.COLS_NAME, column.getName());
            columnJson.put(SchemaProperties.COLS_DATA_TYPE, column.getFieldType().toJson());
            columnJson.put(SchemaProperties.COLS_AUTO_DEFINE, column.getAutoDefineType().getText());
            columnJson.put(SchemaProperties.COLS_INDEX, column.getIndexType().getText());
            colsJson.put(column.getName(), columnJson);
        }

        jsonObject.put(SchemaProperties.COLS, colsJson);
        return jsonObject;
    }

    public String toJSONString() throws JSONException, OperationException {
        return toJSONObject().toString();
    }
    
    public Set<String> getMissingColumnNames(Set<String> keys) throws OperationException {
        Set<String> missingColumnNamesSet = new HashSet<>(keys);
        missingColumnNamesSet.removeAll(map.keySet());
        return missingColumnNamesSet;
    }

    public void addDefaultPrimaryColumn() throws  OperationException{
        IndexTypes indexType = IndexTypes.fromString("unique");
        AutoDefineTypes autoDefineType = AutoDefineTypes.fromString("uuid");
        FieldType fieldType = FieldTypeFactory.fromString("string");
        Column column = new Column("_id", fieldType, indexType, autoDefineType);
        this.getColumnMap().put("_id", column);
        this.setPrimary("_id");
    }

    public List<String> getOrderedVisibleColumnNames() {
        List<Column> columnList = new ArrayList<>(map.values());
        Collections.sort(columnList, new ColumnComparator());

        List<String> columnNamesList = new ArrayList<>();
        columnList.forEach(item -> columnNamesList.add(item.getName()));
        return Collections.unmodifiableList(columnNamesList);
    }

    public List<String> getOrderedInternalColumnNames() {
        List<Column> columnList = new ArrayList<>(map.values());
        Collections.sort(columnList, new ColumnComparator());

        List<String> columnNamesList = new ArrayList<>();
        columnList.forEach(item -> columnNamesList.add(item.getMappedName()));
        return Collections.unmodifiableList(columnNamesList);
    }
}



