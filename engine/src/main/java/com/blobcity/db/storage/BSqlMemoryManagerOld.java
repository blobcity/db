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

package com.blobcity.db.storage;

import com.blobcity.db.bsql.BSqlIndexManager;
import com.blobcity.db.exceptions.ErrorCode;
import com.blobcity.db.exceptions.OperationException;
import com.blobcity.db.memory.old.MemoryTableStore;
import com.blobcity.db.schema.beans.SchemaManager;
import com.blobcity.db.util.FileNameEncoding;
import java.io.IOException;
import java.nio.file.DirectoryStream.Filter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * For managing data stored in memory only
 *
 * @author sanketsarang
 */
@Component
public class BSqlMemoryManagerOld {
    @Autowired
    private SchemaManager schemaManager;
    private static final Logger logger = LoggerFactory.getLogger(BSqlMemoryManagerOld.class);
    
    public boolean exists(final String app, final String table, final String key) {
        String internalTableName = app+"."+table;
        return  MemoryTableStore.getTable(internalTableName).getRecord(key) != null;
    }
    
    public void remove(final String app, final String table, String key)  throws OperationException{
        String internalTableName = app + "." + table;
        MemoryTableStore.getTable(internalTableName).remove(key);
    }

    public void save(final String app, final String table, final String key, final String jsonString) throws OperationException {
        String internalTableName = app + "." + table;
        MemoryTableStore.getTable(internalTableName).save(key, jsonString);
    }

    public void insert(final String app, final String table, final Object key, final Object jsonObj) throws OperationException {
        String internalTableName = app + "." + table;
        MemoryTableStore.getTable(internalTableName).insert(key, jsonObj);
    }
    
    public void insert(final String app, final String table, final String key, final Object jsonWithInternalSchema, final Object json, Map<String, String> viewableToInternal) throws OperationException {
        String internalTableName = app + "." + table;
        MemoryTableStore.getTable(internalTableName).insert(key, jsonWithInternalSchema, json, viewableToInternal);
    }

    public boolean rename(final String app, final String table, final String existingKey, final String newKey) throws OperationException {
        String internalTableName = app + "." + table;
        MemoryTableStore.getTable(internalTableName).rename(existingKey, newKey);
        return true;
    }
    
    public boolean clearContents(final String app, final String table) throws OperationException {
        String internalTableName = app + "." + table;
        MemoryTableStore.getTable(internalTableName).clearAll();
        return true;
    }

    /**
     * <p>
     * Selects the record matching the key from the predefined table. This function currently identifies only the file
     * name as a valid key. The implementation must be changed to allow fetching from any indexed field of the table</p>
     *
     * @param app the application id of the BlobCity application
     * @param table the table within the specified application
     * @param key the key of the record to select
     * @return selected record
     * @throws com.blobcity.db.exceptions.OperationException
     */
    public String select(final String app, final String table, final String key) throws OperationException {
        String internalTableName = app + "." + table;
        if(MemoryTableStore.getTable(internalTableName).getRecord(key) != null) {
            return MemoryTableStore.getTable(internalTableName).getRecord(key).toString();
        }
        throw new OperationException(ErrorCode.PRIMARY_KEY_INEXISTENT, "A record with the given primary key: " + key + " could not be found in table: " + table);
    }

    public Collection<Object> selectAll(final String app, final String table) throws OperationException {
        final String tableName = app + "." + table;
        if(!MemoryTableStore.exists(tableName)) {
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "The requested table does not exist: " + tableName);
        }
        
        return MemoryTableStore.getTable(tableName).getAllRecordsInTbl();
    }
    
    public List<Object> selectAllFromCols(final String app, final String table, List<String> colsToSelect) throws OperationException {
        final String tableName = app + "." + table;
        if(!MemoryTableStore.exists(tableName)) {
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "The requested table does not exist: " + tableName);
        }
        
        return MemoryTableStore.getTable(tableName).getAllRecordsInCols(colsToSelect);
    }
    
    public List<String> selectAllKeys(final String app, final String table) throws OperationException {
        List<String> list = new ArrayList<>();
        String tableName = app + "." + table;
        if(MemoryTableStore.exists(tableName)) {
            Set<Object> allKeys = MemoryTableStore.getTable(tableName).getAllKeys();
            allKeys.stream().forEach((key) -> {
                list.add(key.toString());
            });
        }
        else {
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "An internal operation error occured. Could not select record for table: " + table);
        }
        return list;
    }
    
    /**
     * Gets all primary keys within the specified table in the form of an iterator. Only one key is loaded at any point
     * in this. No select governor limits apply on this function.
     *
     * @param app The application id of the application
     * @param table The table name of the table who's records are to be selected
     * @return An <code>Iterator<String></code> which iterates over primary keys of all records in the table.
     */
    public Iterator<String> selectAllKeysAsStream(final String app, final String table) throws IOException {

        String tableName = app + "." + table;
        try {
            if (MemoryTableStore.exists(tableName)) {
                Set<String> allKeys = MemoryTableStore.getTable(tableName).getAllKeysAsString();
                return allKeys.iterator();
            }
        } catch (Exception e) {
            throw new IOException("selectAllKeysAsStream Error");
        }
        return null;
    }

    /**
     * Gets an iterator over primary keys for all key records that match the filter condition. The filter condition
     * should ideally apply only on values of the primary keys and not values of other columns, for which functions
     * within {@link BSqlIndexManager} should be used instead
     *
     * @param app The application id of the BLobCity application
     * @param table name of table within the BlobCity application
     * @param filter the filter that needs to be satisfied for the returned values
     * @return A <code>Iterator<String></code> containing primary keys that match the filter criteria
     * @throws com.blobcity.db.exceptions.OperationException
     */
    public Iterator<String> selectWithFilterAsStream(final String app, final String table, final Filter filter) throws OperationException {
        try {
            return selectAllKeysAsStream(app, table);
        } catch (IOException ex) {
            java.util.logging.Logger.getLogger(BSqlMemoryManagerOld.class.getName()).log(Level.SEVERE, null, ex);
        }
        return null;
    }

    /**
     * Gets a set of primary keys for all key records that match the filter condition. The filter condition should
     * ideally apply only on values of the primary keys and not values of other columns, for which functions within
     * {@link BSqlIndexManager} should be used instead. This function internally using the streaming (iterator)
     * equivalent.
     *
     * @param app The application id of the BLobCity application
     * @param table name of table within the BlobCity application
     * @param filter the filter that needs to be satisfied for the returned values
     * @return A <code>Set<String></code> containing primary keys that match the filter criteria
     * @throws com.blobcity.db.exceptions.OperationException
     */
    public Set<String> selectWithFilter(final String app, final String table, final Filter filter) throws OperationException {
        Set<String> set = new HashSet<>();
        Iterator<String> iterator = selectWithFilterAsStream(app, table, filter);
        while (iterator.hasNext()) {
            String next = iterator.next().toString();
            set.add(FileNameEncoding.decode(next));
        }
        return set;
    }
    
    public boolean existsTable(final String app, final String table) throws OperationException {
        String tableName = app + "." + table;
        return MemoryTableStore.exists(tableName);
    }
}
