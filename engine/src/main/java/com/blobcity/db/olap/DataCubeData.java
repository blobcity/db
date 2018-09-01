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
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class represents the data structure for storing data cube columns
 *
 *
 * @author sanketsarang
 */
public class DataCubeData implements Serializable {
    // column name mapped to column data
    // name of data cube
    private Map<Object, DataCubeColumn> map;
    private Map<Object, Object> dimMap;
    private static final Logger logger =  LoggerFactory.getLogger(DataCubeData.class);
    
    private String name;

    /**
     *
     */
    public DataCubeData() {
        this.map = new HashMap<>();
        this.dimMap = new HashMap<>();
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
     * @param name
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     *
     * @return
     */
    public Map<Object, DataCubeColumn> getMap() {
        return map;
    }

    /**
     *
     * @param map
     */
    public void setMap(Map<Object, DataCubeColumn> map) {
        if (map == null) {
            return;
        }
        this.map = map;
    }

    /**
     *
     * @param colName
     */
    public void createCol(Object colName) {
        if (!map.containsKey(colName)) {
            DataCubeColumn col = new DataCubeColumn(colName.toString());
            map.put(colName, col);
        }
    }

    /**
     *
     * @param colName
     * @param tableName
     */
    public void createCol(Object colName, Object tableName) {
        if (!map.containsKey(colName)) {
            DataCubeColumn col = new DataCubeColumn(colName.toString());
            col.setTableName(tableName.toString());
            map.put(colName, col);
        }
    }

    public void createDim(Object dimName, Object tableName) throws OperationException {
        if (!dimMap.containsKey(dimName)) {
//            logger.debug("inside createDim: " +dimName.toString());
            DataCubeDimension dim = new DataCubeDimension(dimName.toString());
            //dim.setTableName(tableName.toString());
            dimMap.put(dimName, dim);
        }
    }
    
    /**
     *
     * @param colName
     * @return
     */
    public DataCubeColumn getColumn(final Object colName) {
        return map.get(colName);
    }

    public DataCubeDimension getDimenson(final Object dimName) {
//        logger.debug("dim: " +dimName.toString());
        return (DataCubeDimension)(dimMap.get(dimName.toString()));
    }
    
    /**
     *
     * @return
     */
    public Set<Object> getAllKeys() {
        return map.keySet();
    }

    /**
     *
     * @return
     */
    public Set<Object> getAllColNames() {
        return map.keySet();
    }

    /**
     *
     * @param key
     * @param col
     */
    public void insert(final Object key, final DataCubeColumn col) {
        if (!map.containsKey(key.toString())) {
            map.put(key.toString(), col);
        }
    }

    /**
     *
     * @param key
     * @param col
     */
    public void save(final Object key, final DataCubeColumn col) {
        if (map.containsKey(key)) {
            map.replace(key, col);
        } else {
            map.put(key, col);
        }
    }

    /**
     *
     * @param key
     */
    public void remove(final Object key) {
        map.remove(key);
    }

    /**
     *
     */
    public void clearAll() {
        map.clear();
    }
}
