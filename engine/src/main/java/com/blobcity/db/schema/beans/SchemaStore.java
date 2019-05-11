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
import com.blobcity.db.schema.ColumnMapping;
import com.blobcity.db.schema.Schema;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

/**
 * Persists schema of tables in memory. Schema is identified by application id and table name with the respective application id.
 *
 * @author sanketsarang
 */
@Component
public class SchemaStore {
    private static final Logger  logger = LoggerFactory.getLogger(SchemaStore.class);
    
    private static SchemaStore self;
    @Autowired(required = false)
    @Lazy
    private SchemaManager schemaManager;
    /**
     * AppId -> Table name -> Schema
     */
    private final Map<String, Map<String, Schema>> schemaMap = new HashMap<>();
    /**
     * AppId -> Table name -> Column Mapping
     */
    private final Map<String, Map<String, ColumnMapping>> columnMap = new HashMap<>();
    
    @PostConstruct
    private void init() {
        self = this;
    }
    
    public static final SchemaStore getInstance() {
        return self;
    }

    /**
     * Loads schema of a specific table inside an application. The function will reload any previously loaded schema of the table.
     *
     * @param appId The application id of the application
     * @param table The table name of the table within the specified application.
     * @throws com.blobcity.db.exceptions.OperationException
     */
    public synchronized void loadSchema(final String appId, final String table) throws OperationException {
        final Schema schema = schemaManager.readSchema(appId, table);
        ColumnMapping columnMapping = new ColumnMapping();
        try{
            columnMapping = schemaManager.readColumnMapping(appId, table);
        }
        // this is done because a table can have no columns
        catch(OperationException ex){
            if(ex.getErrorCode() != ErrorCode.SCHEMA_FILE_NOT_FOUND){
                logger.debug(null, ex);
                throw ex;
            }
            else{
                logger.info("No column mapping found for the table " + appId + "," + table);
            }
        }
        Map applicationMap;
        Map columnMappingMap;
        if (schemaMap.containsKey(appId) && columnMap.containsKey(appId)) {
            applicationMap = schemaMap.get(appId);
            columnMappingMap = columnMap.get(appId);
        } else {
            applicationMap = new HashMap<>();
            columnMappingMap=  new HashMap<>();
            schemaMap.put(appId, applicationMap);
            columnMap.put(appId, columnMappingMap);
        }

        applicationMap.put(table, schema);
        columnMappingMap.put(table, columnMapping);
    }

    /**
     * Loads schema of all tables inside the specified application. Only tables present within the default database as defined by
     * <code>BSql.DATABASE_FOLDER_NAME</code> are loaded.
     *
     * @param appId The BlobCity application id of the application
     * @throws com.blobcity.db.exceptions.OperationException
     */
    public void loadAllSchema(final String appId) throws OperationException {
        throw new UnsupportedOperationException("Not yet supported.");
    }

    /**
     * Gets {@link Schema} for the specificed table. If schema for that table is not found in the cache then a load of the same is attempted.
     *
     * @param appId The BlobCity application id of the application
     * @param table The name of table within the application
     * @return {@link Schema} from internal cache of the specified table
     * @throws OperationException if an error occurs which caching schema in memory or if the input is invalid
     */
    public synchronized Schema getSchema(final String appId, final String table) throws OperationException {
        if (!schemaMap.containsKey(appId) || !schemaMap.get(appId).containsKey(table)) {
            loadSchema(appId, table);
        }

        return schemaMap.get(appId).get(table);
    }

    public synchronized ColumnMapping getColumnMapping(final String appId, final String table) throws OperationException {
        if (!schemaMap.containsKey(appId) || !schemaMap.get(appId).containsKey(table)) {
            loadSchema(appId, table);
        }

        return columnMap.get(appId).get(table);
    }

    public synchronized void invalidateSchema(final String appId, final String table) throws OperationException {
        if (schemaMap.containsKey(appId) && schemaMap.get(appId).containsKey(table)) {
            schemaMap.get(appId).remove(table);

            if (schemaMap.get(appId).isEmpty()) {
                schemaMap.remove(appId);
            }
        }
    }

    /**
     * Returns the replication factor for the specified collection if the replication type is
     * {@link com.blobcity.db.schema.ReplicationType.DISTRIBUTED}, else returns -1
     * @param ds name of datastore
     * @param collection name of collection
     * @return the replication factor if the replication type of the table is DISTRIBUTED; -1 for MIRRORED replication
     * type
     * @throws OperationException if an error occurs while loading the schema
     */
    public int getReplicationFactor(final String ds, final String collection) throws OperationException {
        Schema schema = getSchema(ds, collection);

        switch(schema.getReplicationType()) {
            case DISTRIBUTED:
                return schema.getReplicationFactor();
            case MIRRORED:
                return -1;
        }

        return 0;
    }
}
