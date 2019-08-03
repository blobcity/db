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

package com.blobcity.db.bsql;

import com.blobcity.db.bsql.filefilters.EQFilenameFilter;
import com.blobcity.db.bsql.filefilters.GTEQFilenameFilter;
import com.blobcity.db.bsql.filefilters.GTFilenameFilter;
import com.blobcity.db.bsql.filefilters.InFilenameFilter;
import com.blobcity.db.bsql.filefilters.LTEQFilenameFilter;
import com.blobcity.db.bsql.filefilters.LTFilenameFilter;
import com.blobcity.db.bsql.filefilters.LikeFilenameFilter;
import com.blobcity.db.bsql.filefilters.NEQFilenameFilter;
import com.blobcity.db.bsql.filefilters.NotInFilenameFilter;
import com.blobcity.db.bsql.filefilters.OperatorFileFilter;
import com.blobcity.db.cache.QueryResultCache;
import com.blobcity.db.code.CodeExecutor;
import com.blobcity.db.code.triggers.TriggerFunction;
import com.blobcity.db.exceptions.ErrorCode;
import com.blobcity.db.exceptions.OperationException;
import com.blobcity.db.indexing.IndexFactory;
import com.blobcity.db.indexing.IndexingStrategy;
import com.blobcity.db.lang.Operators;
import com.blobcity.db.lang.columntypes.FieldType;
import com.blobcity.db.lang.columntypes.FieldTypeFactory;
import com.blobcity.db.lang.datatypes.converters.TypeConverter;
import com.blobcity.db.lang.datatypes.converters.TypeConverterFactory;
import com.blobcity.db.memory.old.MemorySearch;
import com.blobcity.db.memory.records.JsonRecord;
import com.blobcity.db.operations.OperationLogLevel;
import com.blobcity.db.storage.BSqlMemoryManager;
import com.blobcity.db.storage.BSqlMemoryManagerOld;
import com.blobcity.db.tableau.TableauPublishStore;
import com.blobcity.db.util.ConsumerUtil;
import com.blobcity.lib.data.Record;
import com.blobcity.db.schema.AutoDefineTypes;
import com.blobcity.db.schema.Column;
import com.blobcity.db.schema.ColumnMapping;
import com.blobcity.db.schema.IndexTypes;
import com.blobcity.db.schema.Schema;
import com.blobcity.db.schema.Types;
import com.blobcity.db.schema.beans.SchemaManager;
import com.blobcity.db.schema.beans.SchemaStore;
import com.blobcity.db.storage.BSqlFileManager;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.stereotype.Component;

/**
 * This class has all the functions related to data. This class is irrespective of collection type i.e. on disk or
 * in-memory.
 *
 * @author sanketsarang
 * @author Prikshit Kumar
 */
@Component
@EnableAsync
public class BSqlDataManager {

    private static final Logger logger = LoggerFactory.getLogger(BSqlDataManager.class.getName());

    @Autowired
    @Lazy
    private BSqlCollectionManager collectionManager;
    @Autowired
    private BSqlFileManager fileManager;
    @Autowired
    private BSqlMemoryManager memoryManager;
    @Autowired
    private BSqlMemoryManagerOld memoryManagerOld; //TODO: remove this implementation
    @Autowired
    private SchemaManager schemaManager;
    @Autowired
    @Lazy
    private BSqlIndexManager indexManager;
    @Autowired
    @Lazy
    private IndexFactory indexFactory;
    @Autowired
    @Lazy
    private TypeConverterFactory typeConverterFactory;
    @Autowired
    @Lazy
    private CodeExecutor codeExecutor;
    @Autowired
    private MemorySearch memorySearchExecutor;
    @Lazy
    @Autowired
    private TableauPublishStore tableauPublishStore;
    @Lazy
    @Autowired
    private QueryResultCache queryResultCache;

    /**
     *
     * @param datastore
     * @param collection
     * @param _id: unique _id associated with each row of the collection
     * @return
     * @throws OperationException
     */
    public boolean exists(final String datastore, final String collection, final String _id) throws OperationException {
        if (!collectionManager.isInMemory(datastore, collection)) {
            return fileManager.exists(datastore, collection, _id);
        } else {
            return memoryManagerOld.exists(datastore, collection, _id);
        }
    }

    public JSONObject select(final String datastore, final String collection, final String _id) throws OperationException {
        JSONObject fileJson;
        try {
//            recordLockBean.acquireReadLock(account, collection, _id);
            if (! collectionManager.isInMemory(datastore, collection)) {
                fileJson = new JSONObject(fileManager.select(datastore, collection, _id));
            } else {
                fileJson = new JSONObject(memoryManagerOld.select(datastore, collection, _id));
            }

        } catch (JSONException ex) {
            //TODO: Notify admin
            logger.error(null, ex);
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "An internal operation error occurred. "
                    + "Data for record with primary key: " + _id + " in collection: " + collection + " seems to be corrupted.");

        } finally {
//            recordLockBean.releaseReadLock(account, collection, _id);
        }

        try {
            return schemaManager.internalToViewable(datastore, collection, fileJson);
        } catch (OperationException ex) {
            logger.error(null, ex);
            throw ex;
        }
    }

    public JSONObject select(final String datastore, final String collection, final String _id, final Set<String> columns) throws OperationException {
        JSONObject fileJson;
        //TODO: Optimize to load data from index only rather than from actual record
        try {
//            recordLockBean.acquireReadLock(account, collection, _id);
            if (!collectionManager.isInMemory(datastore, collection)) {
                fileJson = new JSONObject(fileManager.select(datastore, collection, _id));
            } else {
                fileJson = new JSONObject(memoryManagerOld.select(datastore, collection, _id));
            }

        } catch (JSONException ex) {
            //TODO: Notify admin
            logger.error(null, ex);
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "An internal operation error occurred. "
                    + "Data for record with primary key: " + _id + " in collection: " + collection + " seems to be corrupted.");

        } finally {
//            recordLockBean.releaseReadLock(account, collection, _id);
        }

        try {
            JSONObject viewableResultJson = schemaManager.internalToViewable(datastore, collection, fileJson);
            retainSelectedColumns(viewableResultJson, columns);
            return viewableResultJson;
        } catch (OperationException ex) {
            logger.error(null, ex);
            throw ex;
        }
    }

    /**
     * select the record with internal view as seen by database
     *
     * @param datastore
     * @param collection
     * @param _id
     * @return
     * @throws OperationException
     */
    private JSONObject selectInternal(final String datastore, final String collection, final String _id) throws OperationException {
        JSONObject fileJson;
        try {
            if (!collectionManager.isInMemory(datastore, collection)) {
                fileJson = new JSONObject(fileManager.select(datastore, collection, _id));
            } else {
                fileJson = new JSONObject(memoryManagerOld.select(datastore, collection, _id));
            }
        } catch (JSONException ex) {
            //TODO: Notify admin
            logger.error(null, ex);
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "An internation operation error occurred. "
                    + "Data for record with primary key: " + _id + " in collection: " + collection + " seems to be corrupted.");

        }

        return fileJson;
    }

//    public Iterator<Object> selectAll(final String datastore, final String collection) throws OperationException {
//
//        //TODO: See if this function is required.
//        if (!collectionManager.isInMemory(datastore, collection)) {
//            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "An internal operation error occurred. "
//                    + "MemoryTable: " + collection + " in db: " + datastore + " does not exist");
//        }
//        long startTime = System.currentTimeMillis();
//        List<Object> finalJsonObjectList;
//        Set<Record> recordList = memoryManager.selectAll(datastore, collection);
//        long endTime = System.currentTimeMillis();
//        logger.debug("Total time taken to get all records: " + (endTime - startTime));
//
//        if (recordList.isEmpty()) {
//            return Collections.EMPTY_LIST.iterator();
//        }
//
//        startTime = System.currentTimeMillis();
//        finalJsonObjectList = new ArrayList<>();
//
//        recordList.parallelStream().forEach((Object x) -> {
//            finalJsonObjectList.add((JSONObject) x);
//        });
//
//        endTime = System.currentTimeMillis();
//        logger.debug("Total time taken to convert internal to viewable: " + (endTime - startTime));
//        return finalJsonObjectList.iterator();
//    }
    
    public Iterator<Object> selectAllFromCols(final String datastore, final String collection, List<String> colsToSelect) throws OperationException {

        //TODO: See if this function is required.
        // throw new UnsupportedOperationException("Not yet supported.");
        if (!collectionManager.isInMemory(datastore, collection)) {
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "An internal operation error occurred. "
                    + "MemoryTable: " + collection + " in db: " + datastore + " does not exist");
        }
        long startTime = System.currentTimeMillis();
        List<Object> finalJsonObjectList;
        Collection<Object> jsonObjectList = memoryManagerOld.selectAllFromCols(datastore, collection, colsToSelect);
        long endTime = System.currentTimeMillis();
        logger.debug("Total time taken to get all records: " + (endTime - startTime));

        if (jsonObjectList.isEmpty()) {
            return Collections.EMPTY_LIST.iterator();
        }

        startTime = System.currentTimeMillis();
        finalJsonObjectList = new ArrayList<>();

        jsonObjectList.parallelStream().forEach((Object x) -> {
            finalJsonObjectList.add((JSONObject) x);
        });

        endTime = System.currentTimeMillis();
        logger.debug("Total time taken to convert internal to viewable: " + (endTime - startTime));
        return finalJsonObjectList.iterator();
    }

    public Iterator<Object> searchCols(final String datastore, final String collection, List<String> colsToSelect, JSONObject [] whereParams) throws OperationException {

        //TODO: See if this function is required.
        // throw new UnsupportedOperationException("Not yet supported.");
        if (!collectionManager.isInMemory(datastore, collection)) {
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "An internal operation error occurred. "
                    + "MemoryTable: " + collection + " in db: " + datastore + " does not exist");
        }
        
        long startTime = System.currentTimeMillis();
        List<Object> finalJsonObjectList = new ArrayList<>();
        List<Object> jsonObjectList = new ArrayList<>();
        String logicalOperator = null;
        
        for (JSONObject whereParam : whereParams) {
            String column = whereParam.get("column").toString();
            String operator = whereParam.get("operator").toString();
            String value = whereParam.get("value").toString();
            if (operator.equals("LT") || operator.equals("LTEQ") || operator.equals("GT") || operator.equals("GTEQ")
                    || operator.equals("EQ") || operator.equals("NEQ") || operator.equals("IN") || operator.equals("NOT_IN")) {
                Operators op = Operators.fromCode(operator);
                logger.debug("operator: " +op.toString() + ", code: " +op.getCode());
                if ((!(column == null)) && (!column.equals("")) && (!(value == null)) && (!value.equals(""))) {
                    Iterator<Object> itr = selectMemoryRecordsWithPattern(datastore, collection, colsToSelect, column, value, op);
                    itr.forEachRemaining(finalJsonObjectList::add);
                    jsonObjectList.clear();
                    itr.forEachRemaining(jsonObjectList::add);
                    if((logicalOperator != null) && (logicalOperator.equals("intersect"))) {
                        Set<Object> set1 = new HashSet<>();
                        Set<Object> set2 = new HashSet<>();
                        set1.addAll(finalJsonObjectList);
                        set2.addAll(jsonObjectList);
                        set1.retainAll(set2);
                        finalJsonObjectList.clear();
                        finalJsonObjectList.addAll(set1);
                    }
                    else if((logicalOperator != null) && (logicalOperator.equals("union"))) {
                        Set<Object> set1 = new HashSet<>();
                        Set<Object> set2 = new HashSet<>();
                        set1.addAll(finalJsonObjectList);
                        set2.addAll(jsonObjectList);
                        set1.addAll(set2);
                        finalJsonObjectList.clear();
                        finalJsonObjectList.addAll(set1);
                    }
                }
            }
            else if(operator.equals("and") || operator.equals("AND")) {
                logicalOperator = "intersect";
            }
            else if(operator.equals("or") || operator.equals("OR")) {
                logicalOperator = "union";
            }
        }
            
        long endTime = System.currentTimeMillis();
        logger.debug("Total time taken to get records: " + (endTime - startTime));

        if (finalJsonObjectList.isEmpty()) {
            return Collections.EMPTY_LIST.iterator();
        }

        return finalJsonObjectList.iterator();
    }

    /**
     * Gets a stream of primary keys of all records present in the specified collection
     *
     * @param datastore The dsSet name
     * @param collection The collection name of the target collection for selecting the records
     * @return An <code>Iterator<String></code> over primary key of all records present within the specified collection
     * @throws OperationException If the dsSet and collection combination are invalid or if any other error occurs
     * that prevents this operation from being executed successfully. Check {@link ErrorCode} for more details.
     */
    public Iterator<String> selectAllKeysAsStream(final String datastore, final String collection) throws OperationException {
        try {
            if (!collectionManager.isInMemory(datastore, collection)) {
                return fileManager.selectAllKeysAsStream(datastore, collection);
            } else {
                return memoryManagerOld.selectAllKeysAsStream(datastore, collection);
            }
        } catch (IOException ex) {
            logger.error(null, ex);
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "An exception occurred in reading records from the collection");
        }
    }

    public List<String> selectAllKeys(final String datastore, final String collection) throws  OperationException{
        if (!collectionManager.isInMemory(datastore, collection)) {
            return fileManager.selectAll(datastore, collection);
        } else {
            return memoryManagerOld.selectAllKeys(datastore, collection);
        }
    }

    public List<String> selectAllKeysWithLimit(final String datastore, final String collection, final int limit) throws OperationException {
        List<String> keys = new ArrayList<>();
        if(!collectionManager.isInMemory(datastore, collection)) {
            Iterator<String> iterator = null;
            try {
                iterator = fileManager.selectAllKeysAsStream(datastore, collection);
                int count = 0;
                while(count++ < limit && iterator.hasNext()) {
                    keys.add(iterator.next());
                }
            } catch (IOException e) {
                logger.error("Error in reading data from filesystem", e);
            }
        } else {
            throw new OperationException(ErrorCode.OPERATION_NOT_SUPPORTED, "SELECT * with limit not supported yet for in-memory collections");
        }
        return keys;
    }
    
    public Iterator<JSONObject> selectAllAsStream(final String datastore, final String collection) {
        //Iterator<Path> iterator = fileManager.selectAllAsStream(dsSet, collection);
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public Iterator<Object> selectMemoryRecordsWithPattern(final String datastore, final String collection, final List<String> colsToSelect, final String column, final Object referenceValue, final Operators operator) throws OperationException {
        final Schema schema = SchemaStore.getInstance().getSchema(datastore, collection);
        final Types columnDataType;
        TypeConverter typeConverter = null;
        OperatorFileFilter directoryStreamFilter = null;
        Comparable comparableReferenceValue = null;
        Set<Object> referenceValuesSet = null;

        if (collectionManager.isInMemory(datastore, collection)) {
            List<Object> lst = memorySearchExecutor.search(operator, datastore, collection, colsToSelect, column, referenceValue.toString());
            return lst.iterator();
        }
        return Collections.EMPTY_LIST.iterator();
    }
    
    /**
     * <p>
     * Returns an iterator to iterate over primary keys of the specified collection that satisfy the comparison
     * condition that is specified by the comparison operator passed as parameter. The reference value passed as
     * parameter needs to abide by requirements of the operator and various across operators as explained below.
     * </p>
     *
     * <p>
     * For {@link Operators} <code>GT</code>, <code>GTEQ</code>, <code>LT</code>, <code>LTEQ</code>, <code>EQ</code> and
     * <code>NEQ</code> the reference value should be of type same as the column type. This operation is valid only for
     * Numeric and String types.
     * </p>
     *
     * <p>
     * For {@link Operators} <code>IN</code> and <code>NOT IN</code> the reference value should be of type
     * <code>Set<T></code> where <code>T</code> is data type of the target column
     * </p>
     *
     * <p>
     * For {@link Operators} <code>LIKE<code> the reference value should be of type <code>String</code> and must adhere
     * to valid SQL like clause format with wildcard characters syntax support.
     * </p>
     *
     * @param datastore The datastorelication id of the BlobCity datastorelication
     * @param collection name of collection within the datastorelication
     * @param colsToSelect
     * @param column name of column within the collection
     * @param referenceValue value of column which is to be checked with the comparison operator to pick up all records
     * that satisfy the operation condition. The value type needs to map to the requirements of the operator as metioned
     * above
     * @param operator one of {@link Operators} for the comparison operation
     * @return an <code>Iterator<String></code> to iterate over all primary key values that satisfy the search condition
     * @throws OperationException if an exception occurs in reading data for the specified column and in evaluating the
     * search condition
     */
    public Iterator<String> selectKeysWithPattern(final String datastore, final String collection, final List<String> colsToSelect, final String column, final Object referenceValue, final Operators operator) throws OperationException {
        final Schema schema = SchemaStore.getInstance().getSchema(datastore, collection);
        final Types columnDataType;
        TypeConverter typeConverter = null;
        OperatorFileFilter directoryStreamFilter = null;
        Comparable comparableReferenceValue = null;
        Set<Object> referenceValuesSet = null;

        /* Check if column is present */
        if(schema.getColumn(column) == null) {
            return Collections.EMPTY_LIST.iterator();
        }

        /* Check if column being searched is indexed and if not index it */
        if(schema.getColumn(column).getIndexType() == IndexTypes.NONE) {
//            System.out.println("Column field type: " + schema.getColumn(column).getFieldType());
            indexManager.index(datastore, collection, column, IndexTypes.BTREE, OperationLogLevel.ERROR);
        }

        if (collectionManager.isInMemory(datastore, collection)) {
            // return memorySearchExecutor.search(app, table, column, referenceValue.toString(), operator);
            List<Object> lst = memorySearchExecutor.search(operator, datastore, collection, colsToSelect, column, referenceValue.toString());
            List<String> strings = new ArrayList<>();
            lst.stream().forEach((object) -> {
                strings.add(object != null ? object.toString() : null);
            });
            return strings.iterator();
        }

        if(schema.getColumn(column) == null) {
            return Collections.EMPTY_LIST.iterator();
        }

        /* Identify type of operation to perform and perform necessary init*/
        switch (operator) {
            case GT:
            case GTEQ:
            case LT:
            case LTEQ:
            case EQ:
            case NEQ:
                columnDataType = schema.getColumn(column).getFieldType().getType();
                typeConverter = typeConverterFactory.getTypeConverter(columnDataType);

                /* Get reference value of the specified type */
                switch (columnDataType) {
                    //TODO: Support all types here
                    case SMALLINT:
                        comparableReferenceValue = new Short(referenceValue.toString());
                        break;
                    case INT:
                    case INTEGER:
                        comparableReferenceValue = new Integer(referenceValue.toString());
                        break;
                    case LONG:
                    case BIGINT:
                        comparableReferenceValue = new Long(referenceValue.toString());
                        break;
                    case FLOAT:
                        comparableReferenceValue = new Float(referenceValue.toString());
                        break;
                    case DOUBLE:
                        comparableReferenceValue = new Double(referenceValue.toString());
                        break;
                    case BOOLEAN:
                        comparableReferenceValue = (boolean) referenceValue;
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
                        comparableReferenceValue = referenceValue.toString();
                        break;
                    default:
                        throw new OperationException(ErrorCode.INVALID_OPERATOR_USAGE, "Invalid use of comparison operator "
                                + operator.name() + " for column " + column + "  of type " + columnDataType.getType());
                }
                break;
            case IN:
            case NOT_IN:
                columnDataType = schema.getColumn(column).getFieldType().getType();
                typeConverter = typeConverterFactory.getTypeConverter(columnDataType);
                referenceValuesSet = (Set<Object>) referenceValue;
                break;
        }

        /* Create command specific artifacts that are used to executed the query */
        switch (operator) {
            case GT:
                directoryStreamFilter = new GTFilenameFilter(typeConverter, comparableReferenceValue);
                break;
            case LT:
                directoryStreamFilter = new LTFilenameFilter(typeConverter, comparableReferenceValue);
                break;
            case GTEQ:
                directoryStreamFilter = new GTEQFilenameFilter(typeConverter, comparableReferenceValue);
                break;
            case LTEQ:
                directoryStreamFilter = new LTEQFilenameFilter(typeConverter, comparableReferenceValue);
                break;
            case EQ:
                directoryStreamFilter = new EQFilenameFilter(typeConverter, comparableReferenceValue);
                break;
            case NEQ:
                directoryStreamFilter = new NEQFilenameFilter(typeConverter, comparableReferenceValue);
                break;
            case IN:
                directoryStreamFilter = new InFilenameFilter(typeConverter, referenceValuesSet);
                break;
            case NOT_IN:
                directoryStreamFilter = new NotInFilenameFilter(typeConverter, referenceValuesSet);
                break;
            case LIKE:
                directoryStreamFilter = new LikeFilenameFilter(referenceValue.toString());
                break;
        }

        /* Choose datastoreropriate directory. Different for primary and non-primary columns */
        if (schema.getPrimary().equals(column)) {
            List<String> listForIterator;
            String keyToCheck;

            /* Short circuit that prevents directory iterator for EQ and IN clause on primary key */
            try {
                switch (operator) {
                    case EQ:
                        listForIterator = new ArrayList<>();
                        keyToCheck = typeConverter.getValue(referenceValue.toString()).toString();
                        if (!collectionManager.isInMemory(datastore, collection)) {
                            if (fileManager.exists(datastore, collection, keyToCheck)) {
                                listForIterator.add(keyToCheck);
                            }
                        } else {
                            if (memoryManagerOld.exists(datastore, collection, keyToCheck)) {
                                listForIterator.add(keyToCheck);
                            }
                        }
                        return listForIterator.iterator();
                    case IN:
                        listForIterator = new ArrayList<>();
                        for (Object value : referenceValuesSet) {
                            keyToCheck = typeConverter.getValue(value.toString()).toString();
                            if (!collectionManager.isInMemory(datastore, collection)) {
                                if (fileManager.exists(datastore, collection, keyToCheck)) {
                                    listForIterator.add(keyToCheck);
                                }
                            } else {
                                if (memoryManagerOld.exists(datastore, collection, keyToCheck)) {
                                    listForIterator.add(keyToCheck);
                                }
                            }
                        }
                        return listForIterator.iterator();
                }
                if (!collectionManager.isInMemory(datastore, collection)) {
                    return fileManager.selectWithFilterAsStream(datastore, collection, directoryStreamFilter);
                }
                return memoryManagerOld.selectWithFilterAsStream(datastore, collection, directoryStreamFilter);
            } catch (IOException ex) {
                logger.error(null, ex);
                throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR);
            }
        } else {
            return indexManager.readIndexStreamWithFilter(datastore, collection, column, directoryStreamFilter);
        }
    }

    /**
     * <p>
     * Returns an iterator to iterate over records of the specified collection that satisfy the comparison condition
     * that is specified by the comparison operator passed as parameter. The reference value passed as parameter needs
     * to abide by requirements of the operator and various across operators as explained below.
     * </p>
     *
     * <p>
     * For {@link Operators} <code>GT</code>, <code>GTEQ</code>, <code>LT</code>, <code>LTEQ</code>, <code>EQ</code> and
     * <code>NEQ</code> the reference value should be of type same as the column type. This operation is valid only for
     * Numeric and String types.
     * </p>
     *
     * <p>
     * For {@link Operators} <code>IN</code> and <code>NOT IN</code> the reference value should be of type
     * <code>Set<T></code> where T is same type as the column type.
     * </p>
     *
     * <p>
     * For {@link Operators} <code>LIKE<code> the reference value should be of type <code>String</code> and must adhere
     * to valid SQL like clause format with wildcard characters syntax support.
     * </p>
     *
     * @param datastore The datastorelication id of the BlobCity datastorelication
     * @param collection name of collection within the datastorelication
     * @param column name of column within the collection
     * @param referenceValue value of column which is to be checked with the comparison operator to pick up all records
     * that satisfy the operation condition
     * @param operator one of {@link Operators} for the comparison operation
     * @return an <code>Iterator<JSONObject></code> to iterate over all records in JSON form that satisfy the search
     * condition
     * @throws OperationException if an exception occurs in reading data from the specified column and in evaluating the
     * search condition
     */
    public Iterator<JSONObject> selectWithPattern(final String datastore, final String collection, final String column, final Object referenceValue, final Operators operator) throws OperationException {
        final Iterator<String> keysIterator = selectKeysWithPattern(datastore, collection,  null, column, referenceValue, operator);
        Iterator<JSONObject> iterator = new Iterator<JSONObject>() {

            @Override
            public boolean hasNext() {
                return keysIterator.hasNext();
            }

            @Override
            public JSONObject next() {
                String key = null;
                try {
                    key = keysIterator.next();
                    
                    if (!collectionManager.isInMemory(datastore, collection)) {
                        String selectJson = fileManager.select(datastore, collection, key);
                        return schemaManager.internalToViewable(datastore, collection, new JSONObject(selectJson));
                    }

                    String selectJson = memoryManagerOld.select(datastore, collection, key);
                    return schemaManager.internalToViewable(datastore, collection, new JSONObject(selectJson));

                } catch (OperationException ex) {
                    logger.error(null, ex);
                    throw new NoSuchElementException("No record found with key " + key + ". If this operation is not "
                            + "transacted it is possible that the record was deleted post index select and before record read.");
                } catch (JSONException ex) {
                    logger.error(null, ex);
                    throw new NoSuchElementException();
                }
            }

            @Override
            public void remove() {
                keysIterator.remove();
            }
        };

        return iterator;
    }

    public void insert(final String ds, final String collection, final Record record) throws OperationException {
        insert(ds, collection, record.asJson());
    }

    public JSONObject insert(final String datastore, final String collection, final JSONObject jsonObject) throws OperationException {
        queryResultCache.invalidate(datastore, collection);
        Schema schema;
        String primaryKey;
        schema = SchemaStore.getInstance().getSchema(datastore, collection);
        
        IndexTypes indexType = IndexTypes.fromString("unique");
        AutoDefineTypes autoDefineType = AutoDefineTypes.fromString("uuid");
        FieldType fieldType = FieldTypeFactory.fromString("string");
        Column primaryColumn = new Column("_id", fieldType, indexType, autoDefineType);
        
//        if (collectionManager.isInMemory(datastore, collection)) {

            Iterator<String> keys = jsonObject.keys();
            while (keys.hasNext()) {
                String currentColName = keys.next();
                String val = null;
                if (!schema.getColumnMap().containsKey(currentColName)) {
                    indexType = IndexTypes.fromString("none");
                    autoDefineType = AutoDefineTypes.fromString("none");
                    Object colValue = jsonObject.get(currentColName);
//                    if(colValue instanceof JSONArray) {
//                        fieldType = FieldTypeFactory.fromString("array");
//                    } else {
                    fieldType = FieldTypeFactory.fromString("string");
//                    }

                    Column column = new Column(currentColName, fieldType, indexType, autoDefineType);
                    schema.getColumnMap().put(currentColName, column);
                    try {
                        schemaManager.writeSchema(datastore, collection, schema, true);
                    } catch (JSONException ex) {
                        //TODO: Notify admin
                        throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "An internal operation error occurred");
                    }
                }
            }
//        }
        /* Set value of auto defined item by ignoring any value if already set, also add missing auto-defined columns.
         Also checks if indexing is required */
        try {
            for (String columnName : schema.getColumnMap().keySet()) {
                final Column column = schema.getColumn(columnName);
                switch (column.getAutoDefineType()) {
                    case UUID:
                        jsonObject.put(columnName, UUID.randomUUID().toString());
                        break;
                    case TIMESTAMP:
                        jsonObject.put(columnName, System.currentTimeMillis());
                        break;
                }
            }
        } catch (JSONException ex) {

            //TODO: Notify admin
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "An internal operation error occurred " + ex.getMessage());
        }

        //check for primary key
        try {
            primaryKey = jsonObject.getString(schema.getPrimary());
        } catch (JSONException ex) {
            primaryKey = UUID.randomUUID().toString();
        }

        //insert value depending on the primary key mentioned in the query
        if (primaryKey == null || primaryKey.isEmpty()) {
            throw new OperationException(ErrorCode.INSERT_ERROR, "INSERT with a null or empty primary key is not permitted");
        }


        /* Insert new record */

        /* Create JSON with internal columns names. Any columns passed in current JSON that are not in schema will be 
         * ignored and any columns present in schema but not in json will be insertes as empty String values */
        JSONObject fileJson = schemaManager.viewableToInternal(datastore, collection, jsonObject);
        codeExecutor.executeTrigger(datastore, collection, TriggerFunction.BEFORE_INSERT, fileJson);
        JSONObject responseJson = null;
        try {
//            recordLockBean.acquireWriteLock(account, collection, primaryKey);
            if (!collectionManager.isInMemory(datastore, collection)) {
                fileManager.insert(datastore, collection, primaryKey, fileJson.toString());
            } else {
                ColumnMapping map = schemaManager.readColumnMapping(datastore, collection);
                //memoryManagerOld.insert(dsSet, collection, primaryKey, fileJson, map.getViewableNameMap());
                //memoryManagerOld.insert(dsSet, collection, primaryKey, fileJson, jsonObject, map.getViewableNameMap());
                memoryManager.insert(datastore, collection, new JsonRecord(fileJson));
//                memoryManagerOld.insert(datastore, collection, primaryKey, fileJson);
            }
//            rowCountStore.incrementRowCount(dsSet, collection);
            responseJson = schemaManager.internalToViewable(datastore, collection, fileJson);

            /* Update indexes */
            if (schema.isIndexingNeeded()) {
                indexManager.addIndex(datastore, collection, primaryKey, responseJson);
            }

        } finally {
//            recordLockBean.releaseWriteLock(account, collection, primaryKey);
        }

        codeExecutor.executeTrigger(datastore, collection, TriggerFunction.AFTER_INSERT, fileJson);
        tableauPublishStore.notifyDataChange(datastore, collection);
        return responseJson;
    }

    public JSONObject insert(String datastore, String database, String collection, JSONObject jsonObject) throws OperationException {
        queryResultCache.invalidate(datastore, collection);
        Schema schema;
        String primaryKey;
        schema = SchemaStore.getInstance().getSchema(datastore, collection);
        
        IndexTypes indexType = IndexTypes.fromString("unique");
        AutoDefineTypes autoDefineType = AutoDefineTypes.fromString("uuid");
        FieldType fieldType = FieldTypeFactory.fromString("string");
        Column primaryColumn = new Column("_id", fieldType, indexType, autoDefineType);
        /* Set value of auto defined item by ignoring any value if already set, also add missing auto-defined columns.
         Also checks if indexing is required */
        try {
            for (String columnName : schema.getColumnMap().keySet()) {
                final Column column = schema.getColumn(columnName);
                switch (column.getAutoDefineType()) {
                    case UUID:
                        jsonObject.put(columnName, UUID.randomUUID().toString());
                        break;
                    case TIMESTAMP:
                        jsonObject.put(columnName, System.currentTimeMillis());
                        break;
                }
            }
        } catch (JSONException ex) {

            //TODO: Notify admin
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "An internal operation error occurred");
        }

        //check for primary key
        try {
            primaryKey = jsonObject.getString(schema.getPrimary());
        } catch (JSONException ex) {
            throw new OperationException(ErrorCode.INSERT_ERROR, "Attempting to insert without specifying primary key value");
        }

        //insert value depending on the primary key mentioned in the query
        if (primaryKey == null || primaryKey.isEmpty()) {
            throw new OperationException(ErrorCode.INSERT_ERROR, "INSERT with a null or empty primary key is not permitted");
        }

        if (collectionManager.isInMemory(datastore, collection)) {
            Iterator<String> keys = jsonObject.keys();
            while (keys.hasNext()) {
                String currentColName = keys.next();
                String val = null;
                if (!schema.getColumnMap().containsKey(currentColName)) {
                    indexType = IndexTypes.fromString("none");
                    autoDefineType = AutoDefineTypes.fromString("none");
                    fieldType = FieldTypeFactory.fromString("string");
                    Column columnn = new Column(currentColName, fieldType, indexType, autoDefineType);
                    schema.getColumnMap().put(currentColName, columnn);
                    try {
                        schemaManager.writeSchema(datastore, collection, schema, true);
                    } catch (JSONException ex) {
                        //TODO: Notify admin
                        throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "An internal operation error occurred");
                    }
                }
            }
        }
        /* Insert new record */

        /* Create JSON with internal columns names. Any columns passed in current JSON that are not in schema will be 
         * ignored and any columns present in schema but not in json will be insertes as empty String values */
        JSONObject fileJson = schemaManager.viewableToInternal(datastore, collection, jsonObject);
        codeExecutor.executeTrigger(datastore, collection, TriggerFunction.BEFORE_INSERT, fileJson);
        JSONObject responseJson = null;
        try {
//            recordLockBean.acquireWriteLock(account, collection, primaryKey);
            if (!collectionManager.isInMemory(datastore, collection)) {
                fileManager.insert(datastore, collection, primaryKey, fileJson.toString());
            } else {
                ColumnMapping map = schemaManager.readColumnMapping(datastore, collection);
                memoryManagerOld.insert(datastore, collection, primaryKey, fileJson, jsonObject, map.getViewableNameMap());
            }
//            rowCountStore.incrementRowCount(dsSet, collection);
            responseJson = schemaManager.internalToViewable(datastore, collection, fileJson);

            /* Update indexes */
            if (schema.isIndexingNeeded()) {
                indexManager.addIndex(datastore, collection, primaryKey, responseJson);
            }

        } finally {
//            recordLockBean.releaseWriteLock(account, collection, primaryKey);
        }
        codeExecutor.executeTrigger(datastore, collection, TriggerFunction.AFTER_INSERT, fileJson);
        tableauPublishStore.notifyDataChange(datastore, collection);
        return responseJson;
    }

    public void remove(final String datastore, final String collection, String _id) throws OperationException {
        queryResultCache.invalidate(datastore, collection);
        JSONObject fileJson;
        
        try {
//            recordLockBean.acquireWriteLock(account, collection, _id);
            try {
                if (!collectionManager.isInMemory(datastore, collection)) {
                    fileJson = new JSONObject(fileManager.select(datastore, collection, _id));
                } else {
                    fileJson = new JSONObject(memoryManagerOld.select(datastore, collection, _id));
                }
            } catch (JSONException ex) {

                //TODO: Notify admin
                logger.error(null, ex);
                throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "An internation operation error occurred. "
                        + "Data for record with primary key: " + _id + " in collection: " + collection + " seems to be corrupted.");
            }
            // caling before delete trigger
            codeExecutor.executeTrigger(datastore, collection, TriggerFunction.BEFORE_DELETE, fileJson);

            JSONObject viewableJson = schemaManager.internalToViewable(datastore, collection, fileJson);
            indexManager.removeIndex(datastore, collection, _id, viewableJson);

            if (!collectionManager.isInMemory(datastore, collection)) {
                fileManager.remove(datastore, collection, _id);
            } else {
                memoryManagerOld.remove(datastore, collection, _id);
            }
            // calling after delete trigger
            codeExecutor.executeTrigger(datastore, collection, TriggerFunction.AFTER_DELETE, fileJson);
            tableauPublishStore.notifyDataChange(datastore, collection);
//            rowCountStore.decrementRowCount(dsSet, collection);
        } finally {
//            recordLockBean.releaseWriteLock(account, collection, _id);
        }
    }

    @Async
    public void removeAsync(final String datastore, final String collection, final String _id) {
        try {
            remove(datastore, collection, _id);
        } catch (OperationException e) {
            logger.warn(e.getErrorCode() + e.getMessage());
        }
    }

    public void save(final String datastore, final String collection, final JSONObject newJsonObject) throws OperationException {
        queryResultCache.invalidate(datastore, collection);
        Schema schema;
        String primaryKey;
        JSONObject existingJsonObject = null;
        boolean requiresIndexing = false;
        boolean recordExists;
        schema = SchemaStore.getInstance().getSchema(datastore, collection);

        /* Set value of auto defined item by ignoring any value if already set, also add missing auto-defined columns. 
         Also checks if indexing is necessary. */
        try {
            for (String columnName : schema.getColumnMap().keySet()) {
                final Column column = schema.getColumn(columnName);
                switch (column.getAutoDefineType()) {
                    case TIMESTAMP:
                        newJsonObject.put(columnName, System.currentTimeMillis());
                        break;
                }

                if (!requiresIndexing && column.getIndexType() != IndexTypes.NONE && !column.getName().equals(schema.getPrimary())) {
                    requiresIndexing = true;
                }
            }
        } catch (JSONException ex) {

            //TODO: Notify admin
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "An internal operation error occurred");
        }

        //check for primary key
        try {
            primaryKey = newJsonObject.getString(schema.getPrimary());
        } catch (JSONException ex) {
            primaryKey = UUID.randomUUID().toString();
        }

        //insert value depending on the primary key mentioned in the query
        if (primaryKey == null || primaryKey.isEmpty()) {
            primaryKey = UUID.randomUUID().toString();
        }

        /* Check if the operation is an insert or update operation. If recordExists then operation is update else insert */
        if (!collectionManager.isInMemory(datastore, collection)) {
            recordExists = fileManager.exists(datastore, collection, primaryKey);
        } else {
            recordExists = memoryManagerOld.exists(datastore, collection, primaryKey);
        }

        /* Read existing column values for updating indexes */
        try {
//            recordLockBean.acquireWriteLock(account, collection, primaryKey);

            if (requiresIndexing || recordExists) {
                try {
                    if (!collectionManager.isInMemory(datastore, collection)) {
                        existingJsonObject = schemaManager.internalToViewable(datastore, collection, new JSONObject(fileManager.select(datastore, collection, primaryKey)));
                    } else {
                        existingJsonObject = schemaManager.internalToViewable(datastore, collection, new JSONObject(memoryManagerOld.select(datastore, collection, primaryKey)));
                    }
                } catch (JSONException ex) {
                    logger.error("Failed to fetch existingJsonObject", ex);
                    throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "An internal operation error occurred during save");
                }
            }

            /* Loop through to retain existing column values and put default values in case of partial update */
            for (Column column : schema.getColumnMap().values()) {
                if (!newJsonObject.has(column.getName()) && recordExists) {
                    if (existingJsonObject.has(column.getName())) {
                        newJsonObject.put(column.getName(), existingJsonObject.get(column.getName()));
                    } else {
                        newJsonObject.put(column.getName(), JSONObject.NULL);
                    }
                } else if (!newJsonObject.has(column.getName())) {
                    newJsonObject.put(column.getName(), JSONObject.NULL);
                }
            }

            JSONObject fileJson = schemaManager.viewableToInternal(datastore, collection, newJsonObject);
            // no updates has hdatastoreened yet.
            codeExecutor.executeTrigger(datastore, collection, TriggerFunction.BEFORE_UPDATE, existingJsonObject, fileJson);
            if (!collectionManager.isInMemory(datastore, collection)) {
                fileManager.save(datastore, collection, primaryKey, fileJson.toString());
            } else {
                memoryManagerOld.save(datastore, collection, primaryKey, fileJson.toString());
            }
            codeExecutor.executeTrigger(datastore, collection, TriggerFunction.AFTER_UPDATE, existingJsonObject, fileJson);
//            if (!recordExists) {
//                rowCountStore.incrementRowCount(dsSet, collection);
//            }

            /* Update indexes */
            if (requiresIndexing) {
                for (Column column : schema.getColumnMap().values()) {
                    if (column.getIndexType() == IndexTypes.NONE || column.getName().equals(schema.getPrimary())) {
                        continue;
                    }

                    IndexingStrategy indexingStrategy = indexFactory.getStrategy(column.getIndexType());
                    try {

                        /* Remove previous index entry only the operation is an update operation */
                        if (recordExists) {
                            indexingStrategy.remove(datastore, collection, column.getName(), existingJsonObject.get(column.getName()).toString(), existingJsonObject.get(schema.getPrimary()).toString());
                        }
                        indexingStrategy.index(datastore, collection, column.getName(), newJsonObject.get(column.getName()).toString(), newJsonObject.get(schema.getPrimary()).toString());
                    } catch (JSONException ex) {
                        logger.error("Could not index column: " + column.getName() + " in collection: " + collection + " in dsSet: " + datastore, ex);
                        throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "An internal operation error occured while attempting to index column: " + column.getName() + " in collection: " + collection);
                    }
                }
            }
        } catch (JSONException ex) {
            logger.error("An internal operation error occurred while performing save operation for collection: " + collection + " inside dsSet: " + datastore + " for record: " + newJsonObject.toString(), ex);
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR);
        } finally {
//            recordLockBean.releaseWriteLock(account, collection, primaryKey);
        }

        tableauPublishStore.notifyDataChange(datastore, collection);
    }

    @Async
    public void updateAsync(final String datastore, final String collection, final String key, Map<String, Object> columnData) {
        queryResultCache.invalidate(datastore, collection);
        try {
            JSONObject jsonObject = select(datastore, collection, key);
            columnData.forEach((k, v) -> jsonObject.put(k, v));
            save(datastore, collection, jsonObject);
        } catch (OperationException ex) {
            logger.error("An error occurred while asynchronously updating record in " + datastore + "." + collection + " for record " + key, ex);
        }
    }

    private void retainSelectedColumns(final JSONObject viewableJsonData, Set<String> columns) {
        Set<String> toRemoveColumns = new HashSet<>(viewableJsonData.keySet());
        toRemoveColumns.removeAll(columns);
        for (String key : toRemoveColumns) {
            viewableJsonData.remove(key);
        }
    }

    public void clearAllData(final String datastore, final String collection) throws OperationException {
        if (collectionManager.isInMemory(datastore, collection)) {
            memoryManagerOld.clearContents(datastore, collection);
        }

        tableauPublishStore.notifyDataChange(datastore, collection);
    }

    public String determineFileType(String filePath) throws OperationException {
        try {
            BufferedReader br = new BufferedReader(new FileReader(filePath));
            String fLine = br.readLine();
            //comma
            if (fLine.split(",").length != 1) {
                return ",";
            } //tab
            else if (fLine.split("\t").length != 1) {
                return "\t";
            } //space
            else if (fLine.split(" ").length != 1) {
                return " ";
            } else {
                return ",";
            }
        } catch (FileNotFoundException ex) {
            throw new OperationException(ErrorCode.DATA_FILE_NOT_FOUND, "The given file: " + filePath + " could not be found ");
        } catch (IOException ex) {
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "An internal operation error occured");
        }
    }

    public boolean populateTable(final String datastore, final String collection, final String dataFileName, final boolean header,
            final String columnsFileName) throws OperationException, FileNotFoundException, IOException {

        String separateChar = "s";
        if (collectionManager.isInMemory(datastore, collection)) {
            String[] colNames = {};
            boolean noColNames = false;
            Schema schema = null;
            IndexTypes indexType = IndexTypes.fromString("unique");
            AutoDefineTypes autoDefineType = AutoDefineTypes.fromString("uuid");
            FieldType fieldType = FieldTypeFactory.fromString("string");
            Column column = new Column("_id", fieldType, indexType, autoDefineType);
            
            /* Used to set primary key field when first column is added. The first column is defaulted to primary key */
//            if (schema.getColumnMap().size() == 1 && (schema.getPrimary() == null || schema.getPrimary().isEmpty())) {
//                schema.setPrimary(columnName);
//            }
            try {
                schema = schemaManager.readSchema(datastore, collection);
                if (!schema.getColumnMap().containsKey("_id")) {
                   schema.getColumnMap().put("_id", column);
                   schema.setPrimary("_id");
                }
            } catch (OperationException ex) {
                if (ex.getErrorCode() == ErrorCode.SCHEMA_FILE_NOT_FOUND) {
                    schema = new Schema();
                    schema.getColumnMap().put("_id", column);
                    schema.setPrimary("_id");
                } else {
                    throw ex;
                }
            }
            if (!header) {
                try {
                    BufferedReader reader = new BufferedReader(new FileReader(columnsFileName));
                    String cols = reader.readLine();
                    colNames = cols.split(" ");
                    for (int i = colNames.length - 1; i >= 0; i--) {
                        String currentColName = colNames[i];
                        if (!schema.getColumnMap().containsKey(currentColName)) {
                            indexType = IndexTypes.fromString("none");
                            autoDefineType = AutoDefineTypes.fromString("none");
                            fieldType = FieldTypeFactory.fromString("string");
                            Column columnn = new Column(currentColName, fieldType, indexType, autoDefineType);
                            schema.getColumnMap().put(currentColName, columnn);
                        }
                    }
                } catch (FileNotFoundException ex) {
                    throw new OperationException(ErrorCode.DATA_FILE_NOT_FOUND, "The given file: " + dataFileName + " could not be found ");
                } catch (IOException ex) {
                    throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "An internal operation error occured. Could not populate collection: " + collection);
                }
            }
            try {
                schemaManager.writeSchema(datastore, collection, schema, true);
            } catch (JSONException ex) {

                //TODO: Notify admin
                throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "An internal operation error occurred");
            }
            try {
                BufferedReader reader = new BufferedReader(new FileReader(dataFileName));
                String currentLine;
                if (header) {
                    String cols = reader.readLine();
                    colNames = cols.split(separateChar);
                }
                if (colNames.length <= 0) {
                    noColNames = true;
                }
                while ((currentLine = reader.readLine()) != null) {
                    Integer index = 0;
                    Integer valuesIndex = 1;
                    String[] values = currentLine.split(separateChar);
                    if (values.length == 1) {
                       logger.debug("currentLine0: " + currentLine);
                    }
                    JSONObject json = new JSONObject();
                    String key = UUID.randomUUID().toString();
                    json.put("_id", key);
                    for(String value: values) {
                         String currentColName = "";
                        if (noColNames == true) {
                            index++;
                            currentColName = index.toString();
                        } else {
                            currentColName = colNames[index];
                            index = (index + 1) % colNames.length;
                        }

                        valuesIndex++;
                        json.put(currentColName, value);
//                        json.put(valuesIndex.toString(), value);
                        if (!schema.getColumnMap().containsKey(currentColName)) {
                            indexType = IndexTypes.fromString("none");
                            autoDefineType = AutoDefineTypes.fromString("none");
                            fieldType = FieldTypeFactory.fromString("string");
                            Column columnn = new Column(currentColName, fieldType, indexType, autoDefineType);
                            schema.getColumnMap().put(currentColName, columnn);
                            try {
                                schemaManager.writeSchema(datastore, collection, schema, true);
                            } catch (JSONException ex) {

                                //TODO: Notify admin
                                throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "An internal operation error occurred");
                            }
                        }
                    }
                    insert(datastore, collection, json);
                }

                tableauPublishStore.notifyDataChange(datastore, collection);

            } catch (FileNotFoundException ex) {
                throw new OperationException(ErrorCode.DATA_FILE_NOT_FOUND, "The given file: " + dataFileName + " could not be found ");
            } catch (IOException ex) {
                throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "An internal operation error occured. Could not populate collection: " + collection);
            }
        } else {
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "An internal operation error occured. Could not select record for collection: " + collection);
        }
        return false;
    }

    public boolean repopulateTable(final String datastore, final String collection, final String dataFileName, final boolean header,
            final String columnsFileName) throws OperationException, FileNotFoundException, IOException {
        
        logger.debug("dsSet: " + datastore + " collection: " + collection + " data: " + dataFileName + " colFileName: " + columnsFileName);
        if (collectionManager.isInMemory(datastore, collection)) {
            logger.debug("Calling memoryManagerOld clearContents");
            memoryManagerOld.clearContents(datastore, collection);
            return populateTable(datastore, collection, dataFileName, header, columnsFileName);
        }
        return false;
    }

    public void popTable(final String datastore, final String collection, final String dataFile, final boolean schemaInFile, final String schemaFile) throws OperationException {

        String separateChar = determineFileType(dataFile);
        
        if (collectionManager.isInMemory(datastore, collection)) {
            String[] colNames = {};
            boolean noColNames = false;
            Schema schema = null;
            IndexTypes indexType = IndexTypes.fromString("unique");
            AutoDefineTypes autoDefineType = AutoDefineTypes.fromString("uuid");
            FieldType fieldType = FieldTypeFactory.fromString("string");
            Column column = new Column("_id", fieldType, indexType, autoDefineType);
            
            // read schema
            try {
                schema = schemaManager.readSchema(datastore, collection);
                if (!schema.getColumnMap().containsKey("_id") && schema.getPrimary() == null) {
                   schema.getColumnMap().put("_id", column);
                   schema.setPrimary("_id");
                }
                // no schema found, creating a new one
            } catch (OperationException ex) {
                if (ex.getErrorCode() == ErrorCode.SCHEMA_FILE_NOT_FOUND) {
                    schema = new Schema();
                    schema.getColumnMap().put("_id", column);
                    schema.setPrimary("_id");
                } else {
                    throw ex;
                }
            }
            // first line is column names,
            // creating full schema
            if (schemaInFile) {
                try {
                    BufferedReader reader = new BufferedReader(new FileReader(dataFile));
                    String cols = reader.readLine();
                    // get all column names
                    colNames = cols.split(separateChar);
                    for (int i = colNames.length - 1; i >= 0; i--) {
                        String currentColName = colNames[i];
                        if (!schema.getColumnMap().containsKey(currentColName)) {
                            indexType = IndexTypes.fromString("none");
                            autoDefineType = AutoDefineTypes.fromString("none");
                            fieldType = FieldTypeFactory.fromString("string");
                            schema.getColumnMap().put(currentColName, new Column(currentColName, fieldType, indexType, autoDefineType));
                        }
                    }
                } catch (FileNotFoundException ex) {
                    throw new OperationException(ErrorCode.DATA_FILE_NOT_FOUND, "The given file: " + dataFile + " could not be found ");
                } catch (IOException ex) {
                    throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "An internal operation error occured. Could not populate collection: " + collection);
                }
            } // no schema found.
            else {
                //TODO, deal with this later.
            }
            //write schema
            try {
                schemaManager.writeSchema(datastore, collection, schema, true);
            } catch (JSONException ex) {
                throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "An internal operation error occurred");
            }

            // read data from file and put it in the map.
            int cnt = 1;
            try {
                BufferedReader reader = new BufferedReader(new FileReader(dataFile));
                // if schema is in file, then we are assuming that file has a fixed structure and no need for flexible schema
                if (schemaInFile) {
                    // skipping first line
                    reader.readLine();
                    String currentLine;
                    while ((currentLine = reader.readLine()) != null) {
                        String[] values = currentLine.split(separateChar);

                        JSONObject rowData = new JSONObject();
                        for (int i = 0; i < values.length; i++) {
                            rowData.put(colNames[i], values[i]);
                        }
                        insert(datastore, collection, rowData);
                        cnt++;
                        if (cnt % 1000 == 0) {
                            logger.debug(cnt + " lines finished");
                        }
                    }
                }
            } catch (FileNotFoundException ex) {
                throw new OperationException(ErrorCode.DATA_FILE_NOT_FOUND, "The given file: " + dataFile + " could not be found ");
            } catch (IOException ex) {
                throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "An internal operation error occured. Could not populate collection: " + collection);
            }
        } else {
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "Given collection is not a in memory collection");
        }
    }

    public void removeWithPattern(final String datastore, final String collection, final String column, final Object referenceValue, final Operators operator) throws OperationException {
        queryResultCache.invalidate(datastore, collection);
        final Iterator<String> keysIterator = selectKeysWithPattern(datastore, collection,  null, column, referenceValue, operator);
        keysIterator.forEachRemaining(key -> {
            try {
                remove(datastore, collection, key);
            } catch (OperationException e) {
                e.printStackTrace();
            }
        });
        tableauPublishStore.notifyDataChange(datastore, collection);
    }

    public List<JSONObject> selectAll(final String ds, final String collection) throws OperationException {
        final List<JSONObject> resultList = Collections.synchronizedList(new ArrayList<>());
        if(collectionManager.isInMemory(ds, collection)) {
            throw new OperationException(ErrorCode.OPERATION_NOT_SUPPORTED, "In-Memory select all records not yet supported");
        } else {
            selectAllKeys(ds, collection).parallelStream()
                    .forEach(ConsumerUtil.throwsException(key -> resultList.add(select(ds, collection, key)), OperationException.class));
            return resultList;
        }
    }

    public List<JSONObject> selectAll(final String ds, final String collection, final int limit) throws OperationException {
        final List<JSONObject> resultList = Collections.synchronizedList(new ArrayList<>());
        if(collectionManager.isInMemory(ds, collection)) {
            throw new OperationException(ErrorCode.OPERATION_NOT_SUPPORTED, "In-Memory select all records not yet supported");
        } else {
            selectAllKeysWithLimit(ds, collection, limit).parallelStream()
                    .forEach(ConsumerUtil.throwsException(key -> resultList.add(select(ds, collection, key)), OperationException.class));
            return resultList;
        }
    }

    public int getRowCount(final String ds, final String collection) throws OperationException {
        if(collectionManager.isInMemory(ds, collection)) {
            throw new OperationException(ErrorCode.OPERATION_NOT_SUPPORTED, "Operation not yet supported");
        } else {
            return fileManager.rowCount(ds, collection);
        }
    }
}