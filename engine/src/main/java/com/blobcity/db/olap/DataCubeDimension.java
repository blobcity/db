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
import com.blobcity.db.memory.old.MemoryColumn;
import com.blobcity.db.schema.Types;
import com.blobcity.db.util.RegExUtil;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.SortedMap;
import java.util.TreeMap;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.LoggerFactory;;
import org.slf4j.Logger;
/**
 * This is the default class comment, please change it!
 * If you're committing this, your merge WILL NOT BE ACCEPTED. You have been warned!
 *
 * @author sanketsarang
 */
public class DataCubeDimension {
    private static final Logger logger = LoggerFactory.getLogger(DataCubeDimension.class);
    
    private String dimName;
    private Types dimType;
    private SortedMap<Object, HashSet<Object>> dim;
    private boolean indexBuilt;
    private HashSet<Object> dimJson;
    
    public String getColName() {
        return dimName;
    }

    public void setColName(String dimName) {
        this.dimName = dimName;
    }

    public Types getColType() {
        return dimType;
    }

    public void setColType(Types dimType) {
        this.dimType = dimType;
    }

    public DataCubeDimension(String dimName, Types dimType) {
        this.dimName = dimName;
        this.dimType = dimType;
        this.dim = new TreeMap<>();
        this.indexBuilt = false;
        this.dimJson = new HashSet<>();
    }
    
    public DataCubeDimension(String dimName) throws OperationException {
        this.dimName = dimName;
        this.dimType = FieldTypeFactory.fromString("String").getType();
        this.dim = new TreeMap<>();
        this.indexBuilt = false;
        this.dimJson = new HashSet<>();
    }
    
    public DataCubeDimension() throws OperationException {
        this.dimName = null;
        this.dimType = FieldTypeFactory.fromString("String").getType();
        this.dim = new TreeMap<>();
        this.indexBuilt = false;
        this.dimJson = new HashSet<>();
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
     * determines the dimType
     * @param colValue
     * @throws OperationException
     */
    public void validateColValue(final Object colValue) throws OperationException {
        if (colValue == null) {
            return; // null value is accepted
        }
        if((RegExUtil.isPhoneNumber(colValue.toString())) || (RegExUtil.isIPAddress(colValue.toString()))) {
            dimType = Types.STRING;
            return;
        }
        Object val = interpret(colValue);
        if (dimType == null) {
            dimType = FieldTypeFactory.fromString(val.getClass().getSimpleName()).getType();
        }
        if ((dimType.getType().equals("Double") || dimType.getType().equals("DOUBLE"))
                && (val.getClass().getSimpleName().equals("Integer") || val.getClass().getSimpleName().equals("Float")
                || val.getClass().getSimpleName().equals("Short") || val.getClass().getSimpleName().equals("Double"))) {
            dimType = FieldTypeFactory.fromString("Double").getType();
        } else if ((dimType.getType().equals("String") || dimType.getType().equals("STRING"))
                && (val.getClass().getSimpleName().equals("String") || val.getClass().getSimpleName().equals("STRING"))) {
            dimType = FieldTypeFactory.fromString("String").getType();
        } else if (dimType.getType().equals(FieldTypeFactory.fromString(val.getClass().getSimpleName()).getType().toString())) {

        }  else if ((dimType.getType().equals("String") || dimType.getType().equals("STRING"))
                && (val.getClass().getSimpleName().equals("Long") || val.getClass().getSimpleName().equals("LONG"))) {
            dimType = FieldTypeFactory.fromString("String").getType();
        } else if ((dimType.getType().equals("String") || dimType.getType().equals("STRING"))
                && (dimType.getType().equals(FieldTypeFactory.fromString(val.getClass().getSimpleName()).getType().toString()) == false)) {
            dimType = FieldTypeFactory.fromString("String").getType();
        } else {
            logger.debug("dimType is: " + dimType.getType());
            logger.debug("colValue type is: " + FieldTypeFactory.fromString(val.getClass().getSimpleName()).getType().toString());
            if (dimType.getType().equals(FieldTypeFactory.fromString(val.getClass().getSimpleName()).getType().toString()) == false) {
                throw new OperationException(ErrorCode.INDEXING_ERROR, "Error occured during index operation, could not validate colValue");
            }
        }
    }
    
    public List<Object> getAllRecordsFromDim()
    {
        return new ArrayList<>(dimJson);
    }
    public void insert(Object colValue, final Object hierarchicalMap) throws OperationException {
        if ((colValue.equals(null)) || (colValue.toString().equals(null)) || (colValue.toString().equals("[]"))
                || (colValue.toString().equals("")) ) {
            return;
        }

        if ((colValue instanceof JSONObject) || (colValue instanceof JSONArray)) {
            dimJson.add(colValue);
        } else {
            if (!dim.containsKey(colValue.toString())) {
                dim.put(colValue.toString(), new HashSet<>());
            }
            //validateColValue(colValue);
            dim.get(colValue.toString()).add(hierarchicalMap);
            dimJson.add(colValue);
        }

    }
    
    public Object getJsonObjectFromHierarchicalMap(Object hierarchicalMap) {
        Map<Object, Object> rowData = (Map<Object,Object>)(hierarchicalMap);
        Map<Object, Object> jsonMap = new HashMap<>();
        for(Object key: rowData.keySet()) {
            MemoryColumn col = ((MemoryColumn) key);
            Object val = rowData.get(key);
            if(col.getColName().equals(col.getNestedColName())) {
                jsonMap.put(col.getColName(), val);
            }
        }
        return new JSONObject(jsonMap);
    }
    
    public Object getJsonObjectForDim(Object dimName, Object hierarchicalMap) {
        Map<Object, Object> rowData = (Map<Object,Object>)(hierarchicalMap);
        for(Object key: rowData.keySet()) {
            MemoryColumn col = ((MemoryColumn) key);
            Object val = rowData.get(key);
            if(col.getColName().equals(dimName) || col.getNestedColName().equals(dimName)) {
                return val;
            }
        }
        return null;
    }
    
    public void buildIndex() {
        
    }
    
    /**
     *
     * @param op
     * @param colsToSelect
     * @param colValue
     * @return
     * @throws OperationException
     */
    public List<Object> search(Operators op, final List<String> colsToSelect, final Object... colValue) throws OperationException {
        try {
            logger.debug("Inside DataCubeDimension search, dim: " + dimName + ", values length: " + colValue.length + ", type: " +dimType + " op: " +op.toString());
            if (colValue.length == 0) {
                return null;
            }
            if((colsToSelect != null) && (!colsToSelect.isEmpty())) {
                for (int k = 0; k < colsToSelect.size(); k++) {
                    String resultCol = colsToSelect.get(k);
                    logger.debug("colsToSelect: " +resultCol);
                }
            }
            List<Object> result = Collections.synchronizedList(new ArrayList<>());
            Comparable[] comparableReferenceValue = new Comparable[colValue.length];
            if (dim == null) {
                this.dim = new TreeMap<>();
            }
            if (dim.size() == 0) {
                // first time, build index for column
                //iterate over the table
                logger.debug("Building dim index");
                buildIndex();
                indexBuilt = true;
            }
           logger.debug("2: Inside DataCubeDimension search, col: " + dimName + ", values length: " + colValue.length + ", type: " +dimType);
            switch (dimType) {
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
                            + op.name() + " for dimension " + dimName + "  of type " + dimType.getType());
            }

            HashSet<Object> jsonObjs = new HashSet<>();
            boolean getAllCols = ((colsToSelect == null) || (colsToSelect.isEmpty()));
            for (Map.Entry<Object, HashSet<Object>> entry : dim.entrySet()) {
                Comparable comparableColValue = null;
                switch (dimType) {
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
                                + dimName + "  of type " + dimType.getType());
                }

//                logger.debug("comparableColValue: " +comparableColValue + ", comparableReferenceValue[0]: " +comparableReferenceValue[0].toString());
                //Map<Object, Object> rowData = (Map<Object, Object>)(hierarchicalMap);
                logger.debug("num hierarchical maps: " +entry.getValue().size());
                switch (op) {
                    
                    /* Less than operator */
                    case LT:
                        for (int i = 0; i < colValue.length; i++) {
                            // returns negative integer if comparableColValue less than comparableReferenceValue[i]
                            if (comparableColValue.compareTo(comparableReferenceValue[i]) < 0) {
                                if (getAllCols) {
                                    for(int arrIndex=0; arrIndex < entry.getValue().size(); arrIndex++) {
                                        Map<Object, Object> rowData = (Map<Object, Object>)(entry.getValue().toArray()[arrIndex]);
                                        Object jsonObj = getJsonObjectFromHierarchicalMap(rowData);
                                        jsonObjs.add(jsonObj);
                                    }
                                } 
                                else {
                                    for (int k = 0; k < colsToSelect.size(); k++) {
                                        String resultCol = colsToSelect.get(k);
                                        
                                        for (int arrIndex = 0; arrIndex < entry.getValue().size(); arrIndex++) {
                                            Map<Object, Object> rowData = (Map<Object, Object>) (entry.getValue().toArray()[arrIndex]);
                                            Object jsonObj = getJsonObjectForDim(resultCol, rowData);
                                            jsonObjs.add(jsonObj);
                                        }
                                    }
                                }
                            }
                        }

                        break;
                    /* Greater than operator */
                    case GT:
                        for (int i = 0; i < colValue.length; i++) {
                            if (comparableColValue.compareTo(comparableReferenceValue[i]) > 0) {
                                if (getAllCols) {
                                    for(int arrIndex=0; arrIndex < entry.getValue().size(); arrIndex++) {
                                        Map<Object, Object> rowData = (Map<Object, Object>)(entry.getValue().toArray()[arrIndex]);
                                        Object jsonObj = getJsonObjectFromHierarchicalMap(rowData);
                                        jsonObjs.add(jsonObj);
                                    }
                                } 
                                else {
                                    for (int k = 0; k < colsToSelect.size(); k++) {
                                        String resultCol = colsToSelect.get(k);
                                        for (int arrIndex = 0; arrIndex < entry.getValue().size(); arrIndex++) {
                                            Map<Object, Object> rowData = (Map<Object, Object>) (entry.getValue().toArray()[arrIndex]);
                                            Object jsonObj = getJsonObjectForDim(resultCol, rowData);
                                            jsonObjs.add(jsonObj);
                                        }
                                    }
                                }
                            }
                        }
                        break;
                    /* Equals operator */
                    case EQ:
                        for (int i = 0; i < colValue.length; i++) {
                            if (comparableColValue.compareTo(comparableReferenceValue[i]) == 0) {
                                if (getAllCols) {
                                    for(int arrIndex=0; arrIndex < entry.getValue().size(); arrIndex++) {
                                        Map<Object, Object> rowData = (Map<Object, Object>)(entry.getValue().toArray()[arrIndex]);
                                        Object jsonObj = getJsonObjectFromHierarchicalMap(rowData);
                                        jsonObjs.add(jsonObj);
                                        logger.debug("jsonObj: " +jsonObj);
                                    }
                                } 
                                else {
                                    for (int k = 0; k < colsToSelect.size(); k++) {
                                        String resultCol = colsToSelect.get(k);
                                        for (int arrIndex = 0; arrIndex < entry.getValue().size(); arrIndex++) {
                                            Map<Object, Object> rowData = (Map<Object, Object>) (entry.getValue().toArray()[arrIndex]);
                                            Object jsonObj = getJsonObjectForDim(resultCol, rowData);
                                            jsonObjs.add(jsonObj);
                                            logger.debug("resultCol: " +resultCol);
                                            logger.debug("jsonObj for resultCol: " +jsonObj);
                                        }
                                    }
                                }
                            }
                        }
                        break;
                    /* Not-Equals operator */
                    case NEQ:
                        for (int i = 0; i < colValue.length; i++) {
                            if (comparableColValue.compareTo(comparableReferenceValue[i]) != 0) {
                                if (getAllCols) {
                                    for(int arrIndex=0; arrIndex < entry.getValue().size(); arrIndex++) {
                                        Map<Object, Object> rowData = (Map<Object, Object>)(entry.getValue().toArray()[arrIndex]);
                                        Object jsonObj = getJsonObjectFromHierarchicalMap(rowData);
                                        jsonObjs.add(jsonObj);
                                    }
                                } 
                                else {
                                    for (int k = 0; k < colsToSelect.size(); k++) {
                                        String resultCol = colsToSelect.get(k);
                                        for (int arrIndex = 0; arrIndex < entry.getValue().size(); arrIndex++) {
                                            Map<Object, Object> rowData = (Map<Object, Object>) (entry.getValue().toArray()[arrIndex]);
                                            Object jsonObj = getJsonObjectForDim(resultCol, rowData);
                                            jsonObjs.add(jsonObj);
                                        }
                                    }
                                }
                            }
                        }
                        break;
                    /* Less than equals operator */
                    case LTEQ:
                        for (int i = 0; i < colValue.length; i++) {
                            if (comparableColValue.compareTo(comparableReferenceValue[i]) <= 0) {
                                if (getAllCols) {
                                    for(int arrIndex=0; arrIndex < entry.getValue().size(); arrIndex++) {
                                        Map<Object, Object> rowData = (Map<Object, Object>)(entry.getValue().toArray()[arrIndex]);
                                        Object jsonObj = getJsonObjectFromHierarchicalMap(rowData);
                                        jsonObjs.add(jsonObj);
                                    }
                                } 
                                else {
                                    for (int k = 0; k < colsToSelect.size(); k++) {
                                        String resultCol = colsToSelect.get(k);
                                        for (int arrIndex = 0; arrIndex < entry.getValue().size(); arrIndex++) {
                                            Map<Object, Object> rowData = (Map<Object, Object>) (entry.getValue().toArray()[arrIndex]);
                                            Object jsonObj = getJsonObjectForDim(resultCol, rowData);
                                            jsonObjs.add(jsonObj);
                                        }
                                    }
                                }
                            }
                        }
                        break;
                    /* Greater than equals operator */
                    case GTEQ:
                        for (int i = 0; i < colValue.length; i++) {
                            if (comparableColValue.compareTo(comparableReferenceValue[i]) >= 0) {
                                if (getAllCols) {
                                    for(int arrIndex=0; arrIndex < entry.getValue().size(); arrIndex++) {
                                        Map<Object, Object> rowData = (Map<Object, Object>)(entry.getValue().toArray()[arrIndex]);
                                        Object jsonObj = getJsonObjectFromHierarchicalMap(rowData);
                                        jsonObjs.add(jsonObj);
                                    }
                                } 
                                else {
                                    for (int k = 0; k < colsToSelect.size(); k++) {
                                        String resultCol = colsToSelect.get(k);
                                        for (int arrIndex = 0; arrIndex < entry.getValue().size(); arrIndex++) {
                                            Map<Object, Object> rowData = (Map<Object, Object>) (entry.getValue().toArray()[arrIndex]);
                                            Object jsonObj = getJsonObjectForDim(resultCol, rowData);
                                            jsonObjs.add(jsonObj);
                                        }
                                    }
                                }
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
                                if (getAllCols) {
                                    for(int arrIndex=0; arrIndex < entry.getValue().size(); arrIndex++) {
                                        Map<Object, Object> rowData = (Map<Object, Object>)(entry.getValue().toArray()[arrIndex]);
                                        Object jsonObj = getJsonObjectFromHierarchicalMap(rowData);
                                        jsonObjs.add(jsonObj);
                                    }
                                } 
                                else {
                                    for (int k = 0; k < colsToSelect.size(); k++) {
                                        String resultCol = colsToSelect.get(k);
                                        for (int arrIndex = 0; arrIndex < entry.getValue().size(); arrIndex++) {
                                            Map<Object, Object> rowData = (Map<Object, Object>) (entry.getValue().toArray()[arrIndex]);
                                            Object jsonObj = getJsonObjectForDim(resultCol, rowData);
                                            jsonObjs.add(jsonObj);
                                        }
                                    }
                                }
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
                            // do this later
                            //jsonObjs.addAll(pkAll);
                        }
                    }
                    break;
                    /* BETWEEN operator */
                    case BETWEEN:
                        if ((comparableColValue.compareTo(comparableReferenceValue[0]) >= 0)
                                && (comparableColValue.compareTo(comparableReferenceValue[1]) <= 0)) {
                            if (getAllCols) {
                                for (int arrIndex = 0; arrIndex < entry.getValue().size(); arrIndex++) {
                                    Map<Object, Object> rowData = (Map<Object, Object>) (entry.getValue().toArray()[arrIndex]);
                                    Object jsonObj = getJsonObjectFromHierarchicalMap(rowData);
                                    jsonObjs.add(jsonObj);
                                }
                            } else {
                                for (int k = 0; k < colsToSelect.size(); k++) {
                                    String resultCol = colsToSelect.get(k);
                                    for (int arrIndex = 0; arrIndex < entry.getValue().size(); arrIndex++) {
                                        Map<Object, Object> rowData = (Map<Object, Object>) (entry.getValue().toArray()[arrIndex]);
                                        Object jsonObj = getJsonObjectForDim(resultCol, rowData);
                                        jsonObjs.add(jsonObj);
                                    }
                                }
                            }
                        }
                        break;
                    /* NOT BETWEEN operator */
                    case NOT_BETWEEN:
                        if ((comparableColValue.compareTo(comparableReferenceValue[0]) < 0)
                                || (comparableColValue.compareTo(comparableReferenceValue[1]) > 0)) {
                            if (getAllCols) {
                                for (int arrIndex = 0; arrIndex < entry.getValue().size(); arrIndex++) {
                                    Map<Object, Object> rowData = (Map<Object, Object>) (entry.getValue().toArray()[arrIndex]);
                                    Object jsonObj = getJsonObjectFromHierarchicalMap(rowData);
                                    jsonObjs.add(jsonObj);
                                }
                            } else {
                                for (int k = 0; k < colsToSelect.size(); k++) {
                                    String resultCol = colsToSelect.get(k);
                                    for (int arrIndex = 0; arrIndex < entry.getValue().size(); arrIndex++) {
                                        Map<Object, Object> rowData = (Map<Object, Object>) (entry.getValue().toArray()[arrIndex]);
                                        Object jsonObj = getJsonObjectForDim(resultCol, rowData);
                                        jsonObjs.add(jsonObj);
                                    }
                                }
                            }
                        }
                        break;
                }
            }
            result.addAll(jsonObjs);
            
            // now get all records
            logger.debug("num records selected: " + result.size());
//            if (result != null) {
//                MemoryTable memTbl = MemoryTableStore.getTable(tableName);
//                if (memTbl != null) {
//                    List<Object> records = Collections.synchronizedList(memTbl.getAllRecords(result));
//                    return records;
//                }
//            }
            for(Object obj: result) {
                logger.debug("record: " +obj.toString());
            }
            return result;
        } catch (OperationException e) {
            logger.error("search operation on column failed: " +e.toString());
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "search operation on column failed");
        }
        //return null;
    }
}
