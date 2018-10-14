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

package com.blobcity.db.memory.old;

import com.blobcity.db.exceptions.OperationException;
import java.util.HashMap;
import com.blobcity.db.lang.Operators;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.json.JSONObject;
import com.blobcity.db.olap.DataCube;
import com.blobcity.util.json.JsonUtil;
import java.util.Collection;
import java.util.Iterator;
import java.util.logging.Level;
import org.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author sanketsarang
 */
public class MemoryTable {

    private MemoryData data;
//    private Map<String, MemoryIndex> indexMap;
    private String name;
//    private boolean durableWrites;
    private DataCube dataCube;
    private List<Object> colsToIndex; // col names for col index
    private JSONObject finalJson;
    
    private Map<Object, Object> rowStore;
    
    private final Logger logger = LoggerFactory.getLogger(MemoryTable.class);

    /**
     *
     * @param name
     */
    public MemoryTable(String name) {
        this.data = new MemoryData();
        this.name = name;
        this.dataCube = new DataCube();
        this.colsToIndex = new ArrayList<>();
        this.finalJson = new JSONObject();
        this.rowStore = new HashMap<>();
    }

    /**
     *
     */
    public MemoryTable() {
        logger.trace("MemoryTable.MemoryTable() started");
        // method body here
        this.data = new MemoryData();
        this.dataCube = new DataCube();
        this.colsToIndex = new ArrayList<>();
        this.finalJson = new JSONObject();
        this.rowStore = new HashMap<>();
    }

    /**
     *
     * @return
     */
    public List<Object> getColsToIndex() {
        return colsToIndex;
    }

    /**
     *
     * @param colsToIndex
     */
    public void setColsToIndex(List<Object> colsToIndex) {
        this.colsToIndex = colsToIndex;
    }

    /**
     *
     * @param colName
     */
    public void addColToIndex(String colName) {
        colsToIndex.add(colName);
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
        this.dataCube.setName("cube_" + name);
        this.dataCube.setTableName(name);
    }

    /**
     *
     * @return
     */
    public DataCube getDataCube() {
        return dataCube;
    }

    /**
     *
     * @param dataCube
     */
    public void setDataCube(DataCube dataCube) {
        this.dataCube = dataCube;
    }

    /**
     *
     * @return
     */
    public MemoryData getData() {
        return data;
    }

    /**
     *
     * @param data
     */
    public void setData(MemoryData data) {
        this.data = data;
    }

    /**
     *
     * @param key
     * @return
     */
    public Object getRecord(Object key) {
        return data.getRecord(key);
    }

    /**
     *
     * @param keys
     * @return
     */
    public List<Object> getAllRecords(List<Object> keys) {
        List result = new ArrayList<>();
        keys.parallelStream().map((key) -> data.getRecord(key)).filter((val) -> (val != null)).forEach((val) -> {
            result.add(val);
        });
        return result;
    }

    /**
     *
     * @return
     */
    public Collection<Object> getAllRecordsInTbl() {
        return data.getAllRecords();
    }

    public List<Object> getAllRecordsInCols(List <String> colsToSelect) {
        List <Object> recordsInCols = new ArrayList<>();
        
        if(colsToSelect.isEmpty()) {
            return new ArrayList<>(getAllRecordsInTbl());
        }
        for(String dimName: colsToSelect) {
            logger.debug("getting records for dim: " + dimName);
            List <Object> dimRecords = dataCube.getDimension(dimName).getAllRecordsFromDim();
            recordsInCols.addAll(dimRecords);
        }
//        colsToSelect.stream().map((dimName) -> {
//            logger.debug("getting records for dim: " + dimName);
//            return dimName;
//        }).forEach((dimName) -> {
//            List <Object> dimRecords = dataCube.getDimension(dimName).getAllRecordsFromDim();
//            recordsInCols.addAll(dimRecords);
//        });
        return recordsInCols;
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
     * @return
     */
    public Set<String> getAllKeysAsString() {
        return data.getAllKeysAsString();
    }

    /**
     *
     * @param pk
     * @param json
     * @throws OperationException
     */
    public void save(final Object pk, final JSONObject json) throws OperationException {
        JSONObject oldObject = new JSONObject(data.getRecord(pk).toString());
        data.save(pk, json);
        Iterator<String> keys = json.keys();
        Iterator<String> oldKeys = oldObject.keys();
        while (oldKeys.hasNext()) {
            String oldColName = oldKeys.next();
            String oldValue = oldObject.getString(oldColName);
            if (dataCube.indexBuilt(oldColName)) {
                dataCube.remove(oldColName, oldValue, pk);
            }
        }
        while (keys.hasNext()) {
            String colName = keys.next();
            String colValue = json.getString(colName);
            if (dataCube.indexBuilt(colName)) {
                dataCube.insert(colName, (Object) colValue, pk);
            }
        }
    }

    /**
     *
     * @param key
     */
    public void remove(final Object key) {
        data.remove(key);
    }

    /**
     *
     * @param key
     * @param json
     * @throws OperationException
     */
    public void insert(final Object key, final JSONObject json) throws OperationException {
//        data.insert(key, json);
//        MemoryRow row = new MemoryRow();
//        row.insert((Object)json, "");
//        rowStore.put(key, (Object)row);
//
//        finalJson = JsonUtil.getHierarchicalJson(json, "");
//        logger.debug("1-finalJson: " +finalJson.toString());
//        Iterator<Object> keys = finalJson.keys();
//        while (keys.hasNext()) {
//            String colName = keys.next().toString();
//            dataCube.createColumn(colName, name);
//            if ((colsToIndex != null) && (colsToIndex.contains(colName))) {
//                dataCube.buildIndexForCol(colName);
//            }
//            if(dataCube.getColumn(colName).indexBuilt() == true) {
//                 dataCube.getColumn(colName).insertJson(json, key);
//            }
//        }
    }
    
    /**
     *
     * @param key
     * @param json
     * @throws OperationException
     */
    public void insert(final Object key, final Object json) throws OperationException {
        data.insert(key, json);
        MemoryRow row = new MemoryRow();
        row.insert((Object)json, "");
        rowStore.put(key, (Object)row);
        
        // iterate over row
        Map<Object, Object> rowData = (Map<Object, Object>)(row.getRow());
        for(Object col: rowData.keySet()) {
            String dimName = ((MemoryColumn)col).getNestedColName();
            Object jsonObj = rowData.get(col);
//           logger.debug("creating dim: " +dimName);
            dataCube.createDimension(dimName, name);
            
            if ((jsonObj != null) && (jsonObj.toString() != null)) {
//                logger.debug("inserting: " + jsonObj.toString(), "," + jsonObj);
                dataCube.getDimension(dimName).insert(jsonObj, (Object) rowData);
            }
            
        }
//        finalJson = JsonUtil.getHierarchicalJson(json, "");
//        logger.debug("2-finalJson: " +finalJson.toString());
//        Iterator<Object> keys = finalJson.keys();
//        while (keys.hasNext()) {
//            String colName = keys.next().toString();
//            dataCube.createColumn(colName, name);
//            if ((colsToIndex != null) && (colsToIndex.contains(colName))) {
//                dataCube.buildIndexForCol(colName);
//            }
//            if(dataCube.getColumn(colName).indexBuilt() == true) {
//                 dataCube.getColumn(colName).insertJson(json, key);
//            }
//        }
    }

    /**
     *
     * @param key
     * @param json
     * @param viewableToInternal
     * @throws OperationException
     */
    public void insert(final Object key, final Object json, final Map<String, String> viewableToInternal) throws OperationException {
//        data.insert(key, json);
//
//        MemoryRow row = new MemoryRow();
//        row.insert((Object)json, "");
//        rowStore.put(key, (Object)row);
//
//        Iterator<Object> keys = ((JSONObject)json).keys();
//        while (keys.hasNext()) {
//            String colName = keys.next().toString();
////            logger.debug("11-col: " + colName + " internal-name: " + viewableToInternal.get(colName));
//            dataCube.createColumn(colName, name);
//            dataCube.getColumn(colName).setColInternalName(viewableToInternal.get(colName));
//            dataCube.getColumn(colName).setColViewableName(colName);
//            if ((colsToIndex != null) && (colsToIndex.contains(colName))) {
//                dataCube.buildIndexForCol(colName);
//            }
//        }
//
//        if(json instanceof JSONObject) {
//            finalJson = JsonUtil.getHierarchicalJson(json, "");
//        }
//        else if(json instanceof JSONArray) {
//            finalJson = JsonUtil.getHierarchicalJsonFromArray(json, "");
//        }
//        Iterator<Object> keys1 = ((JSONObject) finalJson).keys();
//        while (keys1.hasNext()) {
//            String colName = keys1.next().toString();
//            dataCube.createColumn(colName, name);
//
//            if (colName.contains(".")) {
//                String[] colNameParts = colName.split("\\.");
//                String extensionRemoved = colNameParts[0];
//                for (int i = 1; ((i < colNameParts.length) && (viewableToInternal.get(extensionRemoved) == null)); i++) {
//                    extensionRemoved = extensionRemoved + "." + colNameParts[i];
//                }
//                logger.debug("11-col: " + colName + " internal: " + viewableToInternal.get(extensionRemoved));
//                dataCube.getColumn(colName).setColInternalName(viewableToInternal.get(extensionRemoved));
//                dataCube.getColumn(colName).setColViewableName(extensionRemoved);
//            }
//            if ((colsToIndex != null) && (colsToIndex.contains(colName))) {
//                dataCube.buildIndexForCol(colName);
//            }
//            if(dataCube.getColumn(colName).indexBuilt() == true) {
//                logger.debug("1-inserting into col: " +colName);
//                 dataCube.getColumn(colName).insertJson(json, key);
//            }
//        }
    }
    
    public void dump_row_store() {
        for (Object key : rowStore.keySet()) {
            MemoryRow row = (MemoryRow)rowStore.get(key);
            for (Object colKey : ((Map<Object, Object>)row.getRow()).keySet()) {
                 MemoryColumn col = (MemoryColumn)(colKey);
                 Object colVal = ((Map<Object, Object>)row.getRow()).get(col);
                 logger.debug(col.getColName() + " " + col.getNestedColName() + "/" + colVal.toString());
            }
            
        }
    }
    
    /**
     *
     * @param key
     * @param jsonWithInternalSchema
     * @param json
     * @param viewableToInternal
     * @throws OperationException
     */
    public void insert(final Object key, final Object jsonWithInternalSchema, final Object json, final Map<String, String> viewableToInternal) throws OperationException {
//        data.insert(key, jsonWithInternalSchema);
//
//        MemoryRow row = new MemoryRow();
//        row.insert((Object)json, "");
//        rowStore.put(key, (Object)row);
//
//        Iterator<Object> keys = ((JSONObject)json).keys();
//        while (keys.hasNext()) {
//            String colName = keys.next().toString();
//            dataCube.createColumn(colName, name);
//            logger.debug("22-col: " + colName + " internal-name: " + viewableToInternal.get(colName));
//            dataCube.getColumn(colName).setColInternalName(viewableToInternal.get(colName));
//            dataCube.getColumn(colName).setColViewableName(colName);
//            if ((colsToIndex != null) && (colsToIndex.contains(colName))) {
//                dataCube.buildIndexForCol(colName);
//            }
//        }
//
//        if(json instanceof JSONObject) {
//            finalJson = JsonUtil.getHierarchicalJson(json, "");
//        }
//        else if(json instanceof JSONArray) {
//            finalJson = JsonUtil.getHierarchicalJsonFromArray(json, "");
//        }
//        Iterator<Object> keys1 = ((JSONObject) finalJson).keys();
//        while (keys1.hasNext()) {
//            String colName = keys1.next().toString();
//            dataCube.createColumn(colName, name);
//
//            if (colName.contains(".")) {
//                String[] colNameParts = colName.split("\\.");
//                String extensionRemoved = colNameParts[0];
//                for (int i = 1; ((i < colNameParts.length) && (viewableToInternal.get(extensionRemoved) == null)); i++) {
//                    extensionRemoved = extensionRemoved + "." + colNameParts[i];
//                }
//                logger.debug("22-col: " + colName + " internal-name: " + viewableToInternal.get(extensionRemoved));
//                dataCube.getColumn(colName).setColInternalName(viewableToInternal.get(extensionRemoved));
//                dataCube.getColumn(colName).setColViewableName(extensionRemoved);
//            }
//            if ((colsToIndex != null) && (colsToIndex.contains(colName))) {
//                dataCube.buildIndexForCol(colName);
//            }
//            if(dataCube.getColumn(colName).indexBuilt() == true) {
//                logger.debug("22-inserting into col: " +colName);
//                dataCube.getColumn(colName).insertJson(json, key);
//            }
//            else {
//                logger.debug("22-indexBuilt for col: " +colName + " returned false");
//            }
//        }
//
//        dump_row_store();
    }
    
    /**
     *
     * @param pk
     * @param json
     * @throws OperationException
     */
    public void save(final Object pk, final Object json) throws OperationException {

//        JSONObject oldObject = new JSONObject(data.getRecord(pk).toString());
//        data.save(pk, json);
//
//        Set<Object> keys = ((JSONObject) json).keySet();
//        for (Object oldColName : keys) {
//            String oldValue = oldObject.getString(oldColName.toString());
//            if (dataCube.indexBuilt(oldColName.toString())) {
//                try {
//                    dataCube.insert(oldColName.toString(), (Object) oldValue, pk);
//                } catch (OperationException ex) {
//                    java.util.logging.Logger.getLogger(MemoryTable.class.getName()).log(Level.SEVERE, null, ex);
//                }
//            }
//        }
//
//        finalJson = JsonUtil.getHierarchicalJson(json, "");
//        Set<Object> keys1 = ((JSONObject) finalJson).keySet();
//
//        for (Object colName : keys1) {
//            String colValue = finalJson.getString(colName.toString());
//            if (dataCube.indexBuilt(colName)) {
//                try {
//                    dataCube.insert(colName.toString(), (Object) colValue, pk);
//                } catch (OperationException ex) {
//                    java.util.logging.Logger.getLogger(MemoryTable.class.getName()).log(Level.SEVERE, null, ex);
//                }
//            }
//        }
    }

    /**
     *
     * @param oldKey
     * @param newKey
     */
    public void rename(final Object oldKey, final Object newKey) {
        Object json = data.getRecord(oldKey);
        if (json != null) {
            data.remove(oldKey);
            data.insert(newKey, json.toString());
        }
    }

    /**
     *
     * @throws OperationException
     */
    public void clearAll() throws OperationException {
        data.clearAll();
    }

//    public synchronized Map<String, MemoryIndex> getIndexMap() {
//        return indexMap;
//    }

    /**
     *
     * @param op
     * @param colName
     * @param value
     * @return
     * @throws OperationException
     */
    public List<Object> search(Operators op, Object colName, Object... value) throws OperationException {
        List<Object> result = dataCube.search(op, colName, value);
        return result;
    }

//    public void HashSet<Object> search(Operators op, final Object ... value) {
//        HashSet<Object> result;
//        result = data.search(op, value);
//        return result;
//    }
//    public void setIndexMap(Map<String, MemoryIndex> indexMap) {
//        this.indexMap = indexMap;
//    }
//
//    public boolean isDurableWrites() {
//        return durableWrites;
//    }
//
//    public void setDurableWrites(boolean durableWrites) {
//        this.durableWrites = durableWrites;
//    }
    /* Commit the data in the current form to the file system */

    /**
     *
     */
    
    public void commitData() {

    }
}
