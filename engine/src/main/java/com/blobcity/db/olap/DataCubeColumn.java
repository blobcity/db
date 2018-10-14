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

import com.blobcity.db.exceptions.ErrorCode;
import com.blobcity.db.exceptions.OperationException;
import com.blobcity.db.lang.Operators;
import com.blobcity.db.lang.columntypes.FieldTypeFactory;
import java.util.TreeMap;
import java.util.Set;
import java.util.HashSet;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentHashMap;
import org.json.JSONObject;
import com.blobcity.db.memory.old.MemoryTableStore;
import com.blobcity.db.schema.Types;
import com.blobcity.db.util.RegExUtil;
import com.blobcity.util.json.JsonUtil;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Scanner;
//import java.util.logging.Level;
//import java.util.logging.Logger;
import org.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class represents a column of a datacube. Each column is stored as a column value and a set of primary keys of
 * the in-memory or on-disk table that stores the actual data.
 *
 * @author sanketsarang
 */
@Deprecated
public class DataCubeColumn {

     private static final Logger logger = LoggerFactory.getLogger(DataCubeColumn.class);
     
    // Column Value mapped on set of primary keys which contain that specific value of column
    // first entry is colValue, next is set of primary keys with that value
    private SortedMap<Object, HashSet<Object>> col;
    // name of column
    private String name;
    private String colInternalName;
    private String colViewableName; 
    private String tableName;
    private Types colType;
    private JSONObject finalJson;
    private String[] nameParts;
    private String namePartsExceptLast;
    private String viewableExtensionRemoved;
    private boolean indexBuilt;
    private boolean matched;
    
    
    
    /**
     *
     * @return
     */
    public Types getColType() {
        return colType;
    }

    /**
     *
     * @param colType
     */
    public void setColType(Types colType) {
        this.colType = colType;
    }

    /**
     *
     * @return
     */
    public String getColInternalName() {
        return colInternalName;
    }

    /**
     *
     * @param colInternalName
     */
    public void setColInternalName(String colInternalName) {
        this.colInternalName = colInternalName;
    }

    /**
     *
     * @return
     */
    public String getColViewableName() {
        return colViewableName;
    }

    /**
     *
     * @param colViewableName
     */
    public void setColViewableName(String colViewableName) {
        this.colViewableName = colViewableName;
        viewableExtensionRemoved = colViewableName;
        if (colViewableName.contains(".")) {
            String[] colViewableParts = colViewableName.split("\\.");
            viewableExtensionRemoved = colViewableParts[0];
            for (int i = 1; i < colViewableParts.length; i++) {
                viewableExtensionRemoved = viewableExtensionRemoved + "." + colViewableParts[i];
                if ((nameParts != null) && namePartsExceptLast.equals(viewableExtensionRemoved)) {
                    matched = true;
                    break;
                }
            }
        } 
    }

    /**
     *
     */
    public DataCubeColumn() {
        this.col = new TreeMap<>();
        this.name = null;
        this.colType = null;
        this.finalJson = new JSONObject();
        this.colInternalName = null;
        this.nameParts = null;
        this.namePartsExceptLast = null;
        this.matched = false;
        this.indexBuilt = false;
    }

    /**
     *
     * @param name
     */
    public DataCubeColumn(String name) {
        this.col = new TreeMap<>();
        this.name = name;
        this.colType = null;
        this.colViewableName = name;
        this.finalJson = new JSONObject();
        this.colInternalName = null;
        this.matched = false;
        this.indexBuilt = false;
        this.viewableExtensionRemoved = colViewableName;
        if (name.contains(".")) {
            nameParts = name.split("\\.");
            namePartsExceptLast = nameParts[0];
            for (int i = 1; i < nameParts.length - 1; i++) {
                namePartsExceptLast = namePartsExceptLast + "." + nameParts[i];
            }
        }
        if (colViewableName.contains(".")) {
            String[] colViewableParts = colViewableName.split("\\.");
            viewableExtensionRemoved = colViewableParts[0];
            for (int i = 1; i < colViewableParts.length; i++) {
                viewableExtensionRemoved = viewableExtensionRemoved + "." + colViewableParts[i];
                if ((nameParts != null) && namePartsExceptLast.equals(viewableExtensionRemoved)) {
                    matched = true;
                    break;
                }
            }
        } 
    }

    /**
     *
     * @param val
     * @return
     * @throws IllegalArgumentException
     */
    public Object interpret(Object val) throws IllegalArgumentException {
        String s = val.toString();
        Scanner sc = new Scanner(s);
        return sc.hasNextLong() ? sc.nextLong()
                : sc.hasNextInt() ? sc.nextInt()
                : sc.hasNextDouble() ? sc.nextDouble()
                : sc.hasNextBoolean() ? sc.nextBoolean()
                : sc.hasNextBigInteger() ? sc.nextBigInteger()
                : sc.hasNextFloat() ? sc.nextFloat()
                : sc.hasNextByte() ? sc.nextByte()
                : sc.hasNext() ? sc.next()
                : s;
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
    public String getTableName() {
        return tableName;
    }

    /**
     *
     * @param tableName
     */
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    /**
     *
     * @return
     */
    public Map<Object, HashSet<Object>> getMap() {
        return col;
    }

    /**
     *
     * @return
     */
    public boolean indexBuilt() {
        return ((col.size() > 0) || (indexBuilt));
    }

    /**
     *
     * @param col
     */
    public void setMap(ConcurrentHashMap<Object, HashSet<Object>> col) {
        this.col = (SortedMap<Object, HashSet<Object>>) col;
    }

    /**
     *
     * @param colValue
     * @return
     */
    public HashSet<Object> getRecord(final Object colValue) {
        return col.get(colValue);
    }

    /**
     *
     * @param colValue
     * @return
     */
    public HashSet<Object> getKeysWithColValue(final Object colValue) {
        return col.get(colValue);
    }

    /**
     *
     * @return
     */
    public Set<Object> getAllPrimaryKeysOfCol() {
        Set<Object> allPks = new HashSet<>();
        for (Map.Entry<Object, HashSet<Object>> entry : col.entrySet()) {
            allPks.addAll(entry.getValue());
        }
        return allPks;
    }

    /**
     *
     * @return
     */
    public Set<Object> getAllKeys() {
        return col.keySet();
    }

    /**
     *
     * @param from
     * @param to
     * @return
     */
    public Set<Object> getPKsForRangeOfColValues(Object from, Object to) {
        SortedMap<Object, HashSet<Object>> pkRange = col.subMap(from, to);
        Set<Object> allPks = new HashSet<>();
        for (Map.Entry<Object, HashSet<Object>> entry : pkRange.entrySet()) {
            allPks.addAll(entry.getValue());
        }
        return allPks;
    }

    /**
     * determines the colType
     * @param colValue
     * @throws OperationException
     */
    public void validateColValue(final Object colValue) throws OperationException {
        if (colValue == null) {
            return; // null value is accepted
        }
        if((RegExUtil.isPhoneNumber(colValue.toString())) || (RegExUtil.isIPAddress(colValue.toString()))) {
            colType = Types.STRING;
            return;
        }
        Object val = interpret(colValue);
        if (colType == null) {
            colType = FieldTypeFactory.fromString(val.getClass().getSimpleName()).getType();
        }
        if ((colType.getType().equals("Double") || colType.getType().equals("DOUBLE"))
                && (val.getClass().getSimpleName().equals("Integer") || val.getClass().getSimpleName().equals("Float")
                || val.getClass().getSimpleName().equals("Short") || val.getClass().getSimpleName().equals("Double"))) {
            colType = FieldTypeFactory.fromString("Double").getType();
        } else if ((colType.getType().equals("String") || colType.getType().equals("STRING"))
                && (val.getClass().getSimpleName().equals("String") || val.getClass().getSimpleName().equals("STRING"))) {
            colType = FieldTypeFactory.fromString("String").getType();
        } else if (colType.getType().equals(FieldTypeFactory.fromString(val.getClass().getSimpleName()).getType().toString())) {

        } else {
            logger.debug("colType is: " + colType.getType());
            logger.debug("colValue type is: " + FieldTypeFactory.fromString(val.getClass().getSimpleName()).getType().toString());
            if (colType.getType().equals(FieldTypeFactory.fromString(val.getClass().getSimpleName()).getType().toString()) == false) {
                throw new OperationException(ErrorCode.INDEXING_ERROR, "Error occured during index operation, could not validate colValue");
            }
        }
    }

    /**
     *
     * @param colValue
     * @param pk
     * @throws OperationException
     */
    public void insert(final Object colValue, final Object pk) throws OperationException {
        if (!col.containsKey(colValue)) {
            col.put(colValue, new HashSet<>());
        }
        validateColValue(colValue);
        col.get(colValue).add(pk);
    }

    /**
     *
     * @param colArray
     * @param pk
     * @throws OperationException
     */
    public void insertJSONArray(final Object colArray, final Object pk) throws OperationException {
        if(colArray instanceof JSONArray) {
            for(int i = 0; i < ((JSONArray)colArray).length(); i++) {
                 Object arrayElement =((JSONArray)colArray).get(i);
                 insertJSONArray(arrayElement, pk);
            }
        }
        else {
            insert(colArray, pk);
        }
    }
    
    /**
     *
     * @param oldValue
     * @param colValue
     * @param pk
     * @throws OperationException
     */
    public void save(final Object oldValue, final Object colValue, final Object pk) throws OperationException {
        if (!col.containsKey(colValue)) {
            col.put(colValue, new HashSet<>());
        }
        validateColValue(colValue);
        col.get(oldValue).remove(pk);
        col.get(colValue).add(pk);
    }

    /**
     *
     * @param colValue
     */
    public void remove(final Object colValue) {
        if (!col.containsKey(colValue)) {
            return;
        }
        col.remove(colValue);
    }

    /**
     *
     * @param colValue
     * @param pk
     */
    public void remove(final Object colValue, final Object pk) {
        if (!col.containsKey(colValue)) {
            return;
        }
        col.get(colValue).remove(pk);
    }

    /**
     *
     */
    public void clearAll() {
        col.clear();
    }

    /**
     *
     * @return
     */
    public Integer getIndexSize() {
        return col.size();
    }

    public void insertJson(Object jsonObj, Object pk) throws OperationException
    {
        
//        if (name.contains(".") && (nameParts == null)) {
//            nameParts = name.split("\\.");
//            namePartsExceptLast = nameParts[0];
//            for (int i = 1; i < nameParts.length - 1; i++) {
//                namePartsExceptLast = namePartsExceptLast + "." + nameParts[i];
//            }
//
//            if (colViewableName.contains(".")) {
//                String[] colViewableParts = colViewableName.split("\\.");
//                viewableExtensionRemoved = colViewableParts[0];
//                for (int i = 1; i < colViewableParts.length; i++) {
//                    viewableExtensionRemoved = viewableExtensionRemoved + "." + colViewableParts[i];
//                    if ((nameParts != null) && namePartsExceptLast.equals(viewableExtensionRemoved)) {
//                        matched = true;
//                        break;
//                    }
//                }
//            }
//        }
//
//        Iterator<Object> cols = ((JSONObject)jsonObj).keys();
//        while (cols.hasNext()) {
//            String colName = cols.next().toString();
//            logger.debug("colName: " + colName + ", viewable: " + colViewableName + ", internal: " + colInternalName + ", name:" + name + "\n");
//
//            if (colName.equals(colViewableName) || colName.equals(colInternalName) || (matched) || (name.equals(colViewableName) && colName.equals(colInternalName))
//                    || ((viewableExtensionRemoved != null) && viewableExtensionRemoved.equals(colName))
//                    || ((viewableExtensionRemoved != null) && (nameParts != null) && viewableExtensionRemoved.equals(nameParts[0]))) {
//                Object val = ((JSONObject) jsonObj).get(colName);
//                logger.debug("val: " + val.toString() + "\n");
//                if (val instanceof JSONObject) {
//                    if ((colViewableName.contains(".")) || (colViewableName.equals(namePartsExceptLast))
//                            || (((viewableExtensionRemoved != null) && (nameParts != null) && viewableExtensionRemoved.equals(nameParts[0])))) {
//                        finalJson = JsonUtil.getHierarchicalJson(val, colViewableName + ".");
//                    } else if (colViewableName.equals(name)) {
//                        finalJson = JsonUtil.getHierarchicalJson(val, colViewableName);
//                    }
//                    logger.debug("val.finalJson: " + finalJson.toString() + "\n");
//                    Iterator<Object> internalCols = finalJson.keys();
//                    while (internalCols.hasNext()) {
//                        String internalColName = internalCols.next().toString();
//                        String fullColName = colName + "." + internalColName;
//                        logger.debug("internalColName: " + internalColName + ", name: " + name + ", fullColName: " + fullColName + "\n");
//                        if (internalColName.equals(name)) {
//                            Object value = finalJson.get(internalColName);
//                            logger.debug("inserting val: " + value.toString() + ", key: " + pk);
//                            if (value instanceof JSONArray) {
//                                insertJSONArray(value, pk);
//                            } else {
//                                insert(value, pk);
//                            }
//                        }
//                    }
//                } else if (val instanceof JSONArray) {
//                    if ((colViewableName.contains(".")) || (colViewableName.equals(namePartsExceptLast))
//                            || (((viewableExtensionRemoved != null) && (nameParts != null) && viewableExtensionRemoved.equals(nameParts[0])))) {
//                        finalJson = JsonUtil.getHierarchicalJsonFromArray(val, colViewableName + ".");
//                    } else if (colViewableName.equals(name)) {
//                        finalJson = JsonUtil.getHierarchicalJsonFromArray(val, colViewableName);
//                    }
//                    logger.debug("2-val.finalJson: " + finalJson.toString() + "\n");
//                    Iterator<Object> internalCols = finalJson.keys();
//                    while (internalCols.hasNext()) {
//                        String internalColName = internalCols.next().toString();
//                        String fullColName = colName + "." + internalColName;
//                        logger.debug("internalColName: " + internalColName + ", name: " + name + ", fullColName: " + fullColName + "\n");
//                        if (internalColName.equals(name)) {
//                            Object value = finalJson.get(internalColName);
//                            logger.debug("3-inserting val: " + value.toString() + ", key: " + pk);
//                            if (value instanceof List) {
//                                for (ListIterator<Object> iter1 = ((List) value).listIterator(); iter1.hasNext();) {
//                                    Object element = iter1.next();
//                                    insert(value, pk);
//                                }
//                            } else if (value instanceof JSONArray) {
//                                insertJSONArray(value, pk);
//                            } else {
//                                insert(value, pk);
//                            }
//                        }
//                    }
//                } else if (colViewableName.equals(name)) {
//                    logger.debug("2222-inserting val: " + val.toString() + ", key: " + pk);
//                    insert(val, pk);
//                }
//            }
//        }
    }
    
    /**
     *
     * @throws OperationException
     */
    public void buildIndex() throws OperationException {
        try {
            if ((tableName == null) || (MemoryTableStore.getTable(tableName) == null)) {
                logger.debug("Either tableName is null or table not found in memory table store");
                return;
            }
            
            Iterator<Object> iter = MemoryTableStore.getTable(tableName).getAllKeys().iterator();
            JSONObject jsonObj;
            Object currKey;
            
            while (iter.hasNext()) {
                currKey = iter.next();
                jsonObj = (JSONObject)(MemoryTableStore.getTable(tableName).getData().getRecord(currKey));
                logger.debug("jsonobj: " +jsonObj + "\n");
                insertJson(jsonObj, currKey);
            }
        } catch (NullPointerException e) {
            logger.error("Could not build column index: " +e.toString());
            throw new OperationException(ErrorCode.INDEXING_ERROR, "Could not build column index");
        }
    }

    // build index without internal schema mapping of the original JSON

    /**
     *
     * @throws OperationException
     */
        public void buildIndexUsingOriginalJSONObject() throws OperationException {
//        try {
//            if ((tableName == null) || (MemoryTableStore.getTable(tableName) == null)) {
//                logger.debug("Either tableName is null or table not found in memory table store");
//                return;
//            }
//
//            Iterator<Object> iter = MemoryTableStore.getTable(tableName).getAllKeys().iterator();
//            JSONObject jsonObj;
//            Object currKey;
//            while (iter.hasNext()) {
//                currKey = iter.next();
//                jsonObj = (JSONObject)(MemoryTableStore.getTable(tableName).getData().getRecord(currKey));
//                logger.debug("jsonobj: " +jsonObj + "\n");
//                Iterator<Object> cols = jsonObj.keys();
//                String[] nameParts = null;
//                if (name.contains(".")) {
//                    nameParts = name.split("\\.");
//                }
//
//                while (cols.hasNext()) {
//                    String colName = cols.next().toString();
//                    logger.debug("colName: " +colName + ", viewable: " +colViewableName + ", internal: " +colInternalName + ", name:" +name + "\n");
//                    String viewableExtensionRemoved = null;
//
//
//                    if (colViewableName.contains(".")) {
//                        viewableExtensionRemoved = colViewableName.split("\\.")[0];
//                    }
//
//                    if (colName.equals(colViewableName) || colName.equals(colInternalName) || ((viewableExtensionRemoved != null) && viewableExtensionRemoved.equals(colName))) {
//                        Object val = jsonObj.get(colName);
//                        logger.debug("val: " +val.toString() + "\n");
//                        if (val instanceof JSONObject) {
//                            finalJson = JsonUtil.getHierarchicalJson(val, colName + ".");
//                            logger.debug("val.finalJson: " +finalJson.toString() + "\n");
//                            Iterator<Object> internalCols = finalJson.keys();
//                            while (internalCols.hasNext()) {
//                                String fullColName = internalCols.next().toString();
//                                logger.debug("name: " +name + ", fullColName: " + fullColName + "\n");
//                                if(fullColName.equals(name)) {
//                                    Object value = finalJson.get(fullColName);
//                                    logger.debug("inserting val: " +value.toString() + ", key: " +currKey);
//                                    insert(value, currKey);
//                                }
//                            }
//                        }
//                        else if (val instanceof JSONArray) {
//                            finalJson = JsonUtil.getHierarchicalJsonFromArray((JSONArray)val, colName + ".");
//                            logger.debug("2-val.finalJson: " +finalJson.toString() + "\n");
//                            Iterator<Object> internalCols = finalJson.keys();
//                            while (internalCols.hasNext()) {
//                                String fullColName = internalCols.next().toString();
//                                logger.debug("2-name: " +name + ", fullColName: " + fullColName + "\n");
//                                if(fullColName.equals(name)) {
//                                    Object value = finalJson.get(fullColName);
//                                    logger.debug("2-inserting val: " +value.toString() + ", key: " +currKey);
//                                    if(value instanceof List) {
//                                        for (ListIterator<Object> iter1 = ((List)value).listIterator(); iter1.hasNext(); ) {
//                                            Object element = iter1.next();
//                                            insert(value, currKey);
//                                        }
//                                    }
//                                    else if(value instanceof JSONArray) {
//                                        for(int i = 0; i < ((JSONArray)value).length(); i++) {
//                                            Object arrayElement =((JSONArray)value).get(i);
//                                            insert(arrayElement, currKey);
//                                        }
//                                    }
//                                    else {
//                                        insert(value, currKey);
//                                    }
//                                }
//                            }
//                        }
//                        else if(colName.equals(name)) {
//                            logger.debug("inserting val: " +val.toString() + ", key: " +currKey);
//                            insert(val, currKey);
//                        }
//                    }
//                }
//            }
//        } catch (NullPointerException e) {
//            logger.error("Could not build column index: " +e.toString());
//            throw new OperationException(ErrorCode.INDEXING_ERROR, "Could not build column index");
//        }
    }

    /**
     *
     * @param op
     * @param colValue
     * @return
     * @throws OperationException
     */
    public List<Object> search(Operators op, final Object... colValue) throws OperationException {
        try {
            logger.debug("Inside DataCubeColumn search, col: " + name + ", values length: " + colValue.length + ", type: " +colType);
            if (colValue.length == 0) {
                return null;
            }
            List<Object> result = Collections.synchronizedList(new ArrayList<>());
            Comparable[] comparableReferenceValue = new Comparable[colValue.length];
            if (col == null) {
                this.col = new TreeMap<>();
            }
            if (col.size() == 0) {
                // first time, build index for column
                //iterate over the table
                logger.debug("Building index");
                if(this.colInternalName != null) {
                    buildIndex();
                    indexBuilt = true;
                }
                else {
                    // this is used in unit tests, also useful if we decide to do away with internal schema mapping
                    buildIndexUsingOriginalJSONObject();
                    indexBuilt = true;
                }
            }
           logger.debug("2: Inside DataCubeColumn search, col: " + name + ", values length: " + colValue.length + ", type: " +colType);
            switch (colType) {
                case SMALLINT:
                    for (int i = 0; i < colValue.length; i++) {
                        comparableReferenceValue[i] = new Short(colValue[i].toString());
                    }
                    break;
                case INT:
                case INTEGER:
                    for (int i = 0; i < colValue.length; i++) {
                        comparableReferenceValue[i] = new Integer(colValue[i].toString());
                    }
                    break;
                case LONG:
                case BIGINT:
                    for (int i = 0; i < colValue.length; i++) {
                        comparableReferenceValue[i] = new BigInteger(colValue[i].toString());
                    }
                    break;
                case FLOAT:
                    for (int i = 0; i < colValue.length; i++) {
                        comparableReferenceValue[i] = new Float(colValue[i].toString());
                    }
                    break;
                case DOUBLE:
                    for (int i = 0; i < colValue.length; i++) {
                        comparableReferenceValue[i] = new Double(colValue[i].toString());
                    }
                    break;
                case BOOLEAN:
                    for (int i = 0; i < colValue.length; i++) {
                        comparableReferenceValue[i] = new Boolean(colValue[i].toString());
                    }
                    break;
                case STRING:
                case VARCHAR:
                case CHARACTER_LARGE_OBJECT:
                case CHARACTER_VARYING:
                case CHAR_LARGE_OBJECT:
                case CHAR_VARYING:
                case CLOB:
                case NATIONAL_CHARACTER_LARGE_OBJECT:
                case NATIONAL_CHARACTER_VARYING:
                case NATIONAL_CHAR_VARYING:
                case NCHAR_LARGE_OBJECT:
                case NCHAR_VARYING:
                case CHAR:
                case CHARACTER:
                case NATIONAL_CHAR:
                case NATIONAL_CHARACTER:
                    for (int i = 0; i < colValue.length; i++) {
                        comparableReferenceValue[i] = colValue[i].toString();
                        logger.debug("ref: " +comparableReferenceValue[i] + ", val: " +colValue[i].toString());
                    }
                    break;
                default:
                    throw new OperationException(ErrorCode.INVALID_OPERATOR_USAGE, "Invalid use of comparison operator "
                            + op.name() + " for column " + name + "  of type " + colType.getType());
            }

            HashSet<Object> pks = new HashSet<>();
            for (Map.Entry<Object, HashSet<Object>> entry : col.entrySet()) {
                Comparable comparableColValue = null;
                switch (colType) {
                    case SMALLINT:
                        comparableColValue = new Short(entry.getKey().toString());
                        break;
                    case INT:
                    case INTEGER:
                        comparableColValue = new Integer(entry.getKey().toString());
                        break;
                    case LONG:
                    case BIGINT:
                        comparableColValue = new BigInteger(entry.getKey().toString());
                        break;
                    case FLOAT:
                        comparableColValue = new Float(entry.getKey().toString());
                        break;
                    case DOUBLE:
                        comparableColValue = new Double(entry.getKey().toString());
                        break;
                    case BOOLEAN:
                        comparableColValue = new Boolean(entry.getKey().toString());
                        break;
                    case STRING:
                    case VARCHAR:
                    case CHARACTER_LARGE_OBJECT:
                    case CHARACTER_VARYING:
                    case CHAR_LARGE_OBJECT:
                    case CHAR_VARYING:
                    case CLOB:
                    case NATIONAL_CHARACTER_LARGE_OBJECT:
                    case NATIONAL_CHARACTER_VARYING:
                    case NATIONAL_CHAR_VARYING:
                    case NCHAR_LARGE_OBJECT:
                    case NCHAR_VARYING:
                    case CHAR:
                    case CHARACTER:
                    case NATIONAL_CHAR:
                    case NATIONAL_CHARACTER:
                        for (int i = 0; i < colValue.length; i++) {
                            comparableColValue = entry.getKey().toString();
                            logger.debug("colV: " +comparableColValue + ", entry: " +entry.getKey().toString());
                        }
                        break;
                    default:
                        throw new OperationException(ErrorCode.INVALID_OPERATOR_USAGE, "Invalid column type for column: "
                                + name + "  of type " + colType.getType());
                }

//                logger.debug("comparableColValue: " +comparableColValue + ", comparableReferenceValue[0]: " +comparableReferenceValue[0].toString());
                switch (op) {
                    //HashSet<Object> v = entry.getValue();
                    /* Less than operator */

                    case LT:
                        for (int i = 0; i < colValue.length; i++) {
                            // returns negative integer if comparableColValue less than comparableReferenceValue[i]
                            if (comparableColValue.compareTo(comparableReferenceValue[i]) < 0) {
                                HashSet<Object> pkAll = entry.getValue();
                                pks.addAll(pkAll);
                            }
                        }

                        break;
                    /* Greater than operator */
                    case GT:
                        for (int i = 0; i < colValue.length; i++) {
                            if (comparableColValue.compareTo(comparableReferenceValue[i]) > 0) {
                                HashSet<Object> pkAll = entry.getValue();
                                pks.addAll(pkAll);
                            }
                        }
                        result.addAll(pks);
                        break;
                    /* Equals operator */
                    case EQ:
                        for (int i = 0; i < colValue.length; i++) {
                            if (comparableColValue.compareTo(comparableReferenceValue[i]) == 0) {
                                HashSet<Object> pkAll = entry.getValue();
                                pks.addAll(pkAll);
                            }
                        }
                        break;
                    /* Not-Equals operator */
                    case NEQ:
                        for (int i = 0; i < colValue.length; i++) {
                            if (comparableColValue.compareTo(comparableReferenceValue[i]) != 0) {
                                HashSet<Object> pkAll = entry.getValue();
                                pks.addAll(pkAll);
                            }
                        }
                        break;
                    /* Less than equals operator */
                    case LTEQ:
                        for (int i = 0; i < colValue.length; i++) {
                            if (comparableColValue.compareTo(comparableReferenceValue[i]) <= 0) {
                                HashSet<Object> pkAll = entry.getValue();
                                pks.addAll(pkAll);
                            }
                        }
                        break;
                    /* Greater than equals operator */
                    case GTEQ:
                        for (int i = 0; i < colValue.length; i++) {
                            if (comparableColValue.compareTo(comparableReferenceValue[i]) >= 0) {
                                HashSet<Object> pkAll = entry.getValue();
                                pks.addAll(pkAll);
                            }
                        }
                        break;
                    /* LIKE operator */
                    case LIKE:
                        break;
                    /* IN operator */
                    case IN:
                        /**
                         * Used to perform a NOT IN type search. This is not a standard SQL operator, but use would in
                         * queries of the type SELECT * FROM table WHERE NOT col IN (val1, val2)
                         */
                        for (int i = 0; i < colValue.length; i++) {
                            if (comparableColValue.compareTo(comparableReferenceValue[i]) == 0) {
                                HashSet<Object> pkAll = entry.getValue();
                                pks.addAll(pkAll);
                            }
                        }
                        break;
                    case NOT_IN: {
                        boolean notIn = true;
                        HashSet<Object> pkAll = entry.getValue();
                        for (int i = 0; i < colValue.length; i++) {
                            if (comparableColValue.compareTo(comparableReferenceValue[i]) == 0) {
                                notIn = false;
                            }
                        }
                        if (notIn == true) {
                            pks.addAll(pkAll);
                        }
                    }
                    break;
                    /* BETWEEN operator */
                    case BETWEEN:
                        if ((comparableColValue.compareTo(comparableReferenceValue[0]) >= 0)
                                && (comparableColValue.compareTo(comparableReferenceValue[1]) <= 0)) {
                            HashSet<Object> pkAll = entry.getValue();
                            pks.addAll(pkAll);
                        }
                        break;
                    /* NOT BETWEEN operator */
                    case NOT_BETWEEN:
                        if ((comparableColValue.compareTo(comparableReferenceValue[0]) < 0)
                                || (comparableColValue.compareTo(comparableReferenceValue[1]) > 0)) {
                            HashSet<Object> pkAll = entry.getValue();
                            pks.addAll(pkAll);
                        }
                        break;
                }
            }
            result.addAll(pks);
            
            // now get all records
            logger.debug("num records selected: " + result.size());
//            if (result != null) {
//                MemoryTable memTbl = MemoryTableStore.getTable(tableName);
//                if (memTbl != null) {
//                    List<Object> records = Collections.synchronizedList(memTbl.getAllRecords(result));
//                    return records;
//                }
//            }
            return result;
        } catch (OperationException e) {
            logger.error("search operation on column failed: " +e.toString());
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "search operation on column failed");
        }
        //return null;
    }
}
