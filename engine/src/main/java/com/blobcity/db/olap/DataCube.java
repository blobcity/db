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

import com.blobcity.db.exceptions.OperationException;
import com.blobcity.db.lang.Operators;
import java.util.List;
import java.util.Set;
import org.json.JSONObject;
import org.slf4j.LoggerFactory;

/**
 * This class represents an in-memory data cube..
 *
 * @author sanketsarang
 */
public class DataCube {
    
    // Data stored in cube
    private DataCubeData data;
    // data cube name
    private String name;
    // name of table
    private String tableName;
    //    private boolean durableWrites;

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(DataCube.class.getName());

    /**
     *
     */
    public DataCube() {
        this.data = new DataCubeData();
    }

    /**
     *
     * @return
     */
    public String getName() {
        return name;
    }

    /**
     *
     * @return
     */
    public String getTableName() {
        return tableName;
    }

    /**
     *
     * @param name
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     *
     * @param name
     */
    public void setTableName(String name) {
        this.tableName = name;
    }

    /**
     *
     * @return
     */
    public DataCubeData getData() {
        return data;
    }

    /**
     *
     * @param data
     */
    public void setData(DataCubeData data) {
        this.data = data;
    }

    /**
     *
     * @param colName
     */
    public void createColumn(Object colName) {
        this.data.createCol(colName);
    }

    /**
     *
     * @param colName
     * @param tableName
     */
    public void createColumn(Object colName, Object tableName) {
        data.createCol(colName, tableName);
    }

    public void createDimension(Object dimName, Object tableName) throws OperationException {
        data.createDim(dimName, tableName);
    }
    
    /**
     *
     * @param colName
     * @return
     */
    public DataCubeColumn getColumn(Object colName) {
        return data.getColumn(colName);
    }
    
    /**
     *
     * @param dimName
     * @return
     */
    public DataCubeDimension getDimension(Object dimName) {
        return data.getDimenson(dimName);    
    }

    /**
     *
     * @param colName
     * @return
     */
    public boolean indexBuilt(Object colName) {
        return data.getColumn(colName).indexBuilt();
    }

    /**
     *
     * @return
     */
    public Set<Object> getAllKeys() {
        return data.getAllKeys();
    }

    /**
     *
     * @param colName
     * @param colValue
     * @param pk
     * @throws OperationException
     */
    public void insert(final Object colName, final Object colValue, final Object pk) throws OperationException {
        data.getColumn(colName).insert(colValue, pk);
    }

    /**
     *
     * @param colName
     * @param colValue
     * @param pk
     * @throws OperationException
     */
    public void insert(final Object colName, final Object colValue, final JSONObject pk) throws OperationException {
        data.getColumn(colName).insert(colValue, pk);
    }

    /**
     *
     * @param colName
     * @param oldValue
     * @param colValue
     * @param pk
     * @throws OperationException
     */
    public void save(final Object colName, final Object oldValue, final Object colValue, final Object pk) throws OperationException {
        data.getColumn(colName).save(oldValue, colValue, pk);
    }

    /**
     *
     * @param colName
     * @param colValue
     * @param pk
     */
    public void remove(final Object colName, final Object colValue, final Object pk) {
        data.getColumn(colName).remove(colValue, pk);
    }

    /**
     *
     * @param oldName
     * @param newName
     */
    public void rename(final Object oldName, final String newName) {
    }

    /**
     *
     * @param colName
     * @throws OperationException
     */
    public void buildIndexForCol(Object colName) throws OperationException {
        if(getColumn(colName).getColInternalName() != null) {
            getColumn(colName).buildIndex();
        }
        else {
            getColumn(colName).buildIndexUsingOriginalJSONObject();
        } 
            
    }

    /**
     *
     */
    public void clearAll() {
        data.clearAll();
    }

    /**
     *
     * @param op
     * @param colName
     * @param colValue
     * @return
     * @throws OperationException
     */
    public List<Object> search(Operators op, final Object colName, final Object... colValue) throws OperationException {
        if (data != null) {
            logger.debug("Inside DataCube search" + " col: " +colName.toString());
            DataCubeColumn cubeCol = data.getColumn(colName);
            List<Object> result = cubeCol.search(op, colValue);
            return result;
        }
        return null;
    }
    
    public List<Object> searchDim(Operators op, final List<String> colsToSelect, final Object dimName, final Object... colValue) throws OperationException {
        if (data != null) {
            logger.debug("Inside DataCube search" + " col: " +dimName.toString());
            DataCubeDimension cubeDim = data.getDimenson(dimName);
            List<Object> result = cubeDim.search(op, colsToSelect, colValue);
            return result;
        }
        return null;
    }

//    public boolean isDurableWrites() {
//        return durableWrites;
//    }
//
//    public synchronized void setDurableWrites(boolean durableWrites) {
//        this.durableWrites = durableWrites;
//    }

    /* Commit the data in the current form to the file system */

    /**
     *
     */
    
    public void commitData() {

    }
}
