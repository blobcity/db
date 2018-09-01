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

package com.blobcity.db.olap;

import com.blobcity.db.bsql.BSqlCollectionManager;
import com.blobcity.db.exceptions.ErrorCode;
import com.blobcity.db.exceptions.OperationException;
import com.blobcity.db.memory.old.MemoryTableStore;
import com.blobcity.db.schema.beans.SchemaManager;
import com.blobcity.db.storage.BSqlMemoryManagerOld;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

/**
 *  This class manages all the data cube related operations
 * 
 * @author sanketsarang
 */
@Component
public class DataCubeManager {
    private static final Logger logger = LoggerFactory.getLogger(DataCubeManager.class);
    
    @Autowired @Lazy
    private BSqlMemoryManagerOld memoryManager;
    @Autowired @Lazy
    private SchemaManager schemaManager;
    @Autowired @Lazy
    private BSqlCollectionManager tableManager;
        
    
    // basic data cube operations
    
    /**
     *
     * @param app
     * @param table
     * @param cols
     * @throws OperationException
     */
        
    public void createDataCube(final String app, final String table, final String... cols) throws OperationException {
        try {
            DataCube cube;
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < cols.length; i++) {
                sb.append(cols[i]);
                if (i < cols.length - 1) {
                    sb.append("_");
                }
            }
            String tableCols = sb.toString();
            String cubeName = app + "_" + table + "_" + tableCols;
            logger.debug("Cubename is: " + cubeName);
            if (tableManager.isInMemory(app, table)) {
                String tableName = app + "." + table;
                cube = DataCubeStore.addCube(cubeName, tableName);
                Set<Object> keys = MemoryTableStore.getTable(tableName).getAllKeys();
                for (Object key : keys) {
                    Object recordJson = MemoryTableStore.getTable(tableName).getRecord(key);
                    JSONObject rowData = new JSONObject(recordJson.toString());
                    Iterator<?> jsonCols = rowData.keys();
                    while (jsonCols.hasNext()) {
                        String internalColName = (String) jsonCols.next();
                        String colName = schemaManager.readColumnMapping(app, table).getViewableName(internalColName);
                        if (tableCols.contains(colName)) {
                            String colValue = rowData.get(internalColName).toString();
                            cube.insert(colName, colValue, key);
                        }
                    }
                }

            } else {
                // on disk table: TBD
            }
        } catch (OperationException ex) {
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "The data cube in db: " + app + " using table: " + table + " could not be created ");
        }
    }
    
    /**
     * delete a dataCube from the memory with given name
     * 
     * @param cubeName
     * @throws OperationException 
     */
    public void deleteDataCube(final String cubeName) throws OperationException {
        if(exists(cubeName)){
            DataCubeStore.delete(cubeName);
        }
        else{
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "The data cube: " + cubeName + " doesn't exist");
        }
    }
    
    /**
     * whether a given dataCube isPresent or not
     * 
     * @param dataCubeName
     * @return
     * @throws OperationException 
     */
    public boolean exists(final String dataCubeName) throws OperationException{
        return DataCubeStore.exists(dataCubeName);
    }
    
    /**
     * returns the list of all dataCubes created
     * 
     * @return
     * @throws OperationException 
     */
    public Set<String> listDataCubes() throws OperationException{
        return DataCubeStore.listDataCubes();
    }
    
    /**
     * returns the list of all dataCubes in a given database
     * 
     * @param app
     * @return
     * @throws OperationException 
     */
    public List<String> listDataCube(final String app) throws OperationException{
        Iterator<String> allCubes = DataCubeStore.listDataCubes().iterator();
        List<String> cubeList = new ArrayList<>();
        String curCube;
        while(allCubes.hasNext()){
            curCube = allCubes.next();
            if(curCube.contains(app)) cubeList.add(curCube);
        }
        return cubeList;
    }
    
    /**
     * rename a dataCube
     * @param oldName
     * @param newName
     * @throws OperationException 
     */
    public void renameDataCube(final String oldName, final String newName) throws OperationException{
        if(!exists(oldName)){
            throw new OperationException(ErrorCode.INVALID_QUERY_PARAMETER, "No such data cube isPresent");
        }
        if(exists(newName)){
            throw new OperationException(ErrorCode.INVALID_QUERY_PARAMETER, "A data cube already isPresent with given name");
        }
        DataCubeStore.rename(oldName, newName);
    }
    
    
    // data cube update operations

    /**
     *
     * @param app
     * @param table
     * @param column
     * @throws OperationException
     */
        public void addColumn(final String app, final String table, final String column) throws OperationException{
        // get schema        
    } 
    
    /**
     *
     * @param app
     * @param table
     * @param column
     * @throws OperationException
     */
    public void removeColumn(final String app, final String table, final String column) throws OperationException{
        
    }
    
    /**
     *
     * @param app
     * @param table
     * @param oldName
     * @param newName
     * @throws OperationException
     */
    public void renameColumn(final String app, final String table, final String oldName, final String newName) throws OperationException{
        
    }
    
}
