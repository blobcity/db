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

import com.blobcity.db.bsql.filefilters.OperatorFileFilter;
import com.blobcity.db.constants.BSql;
import com.blobcity.db.exceptions.ErrorCode;
import com.blobcity.db.exceptions.OperationException;
import com.blobcity.db.global.live.GlobalLiveStore;
import com.blobcity.db.indexing.IndexFactory;
import com.blobcity.db.indexing.IndexingStrategy;
import com.blobcity.db.operations.OperationLogLevel;
import com.blobcity.db.operations.OperationStatus;
import com.blobcity.db.operations.OperationTypes;
import com.blobcity.db.operations.OperationsManager;
import com.blobcity.db.schema.Column;
import com.blobcity.db.schema.IndexTypes;
import com.blobcity.db.schema.Schema;
import com.blobcity.db.schema.beans.SchemaManager;
import com.blobcity.db.schema.beans.SchemaStore;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Iterator;

import com.blobcity.db.sql.util.PathUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.beans.factory.annotation.Autowired;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.context.annotation.Lazy;

/**
 *
 * @author sanketsarang
 */
@Component
public class BSqlIndexManager {

    private static final Logger logger = LoggerFactory.getLogger(BSqlIndexManager.class.getName());

    @Autowired
    private SchemaManager schemaManager;
    @Autowired
    private SchemaStore schemaStore;
    @Autowired
    @Lazy
    private OperationsManager operationsManager;
    @Autowired
    private IndexFactory indexFactory;
    @Autowired
    @Lazy
    private GlobalLiveStore globalLiveStore;

    public String index(final String app, final String table, final String columnName, final IndexTypes indexTypes, final OperationLogLevel operationLogLevel) throws OperationException {
        Schema schema = schemaManager.readSchema(app, table);

        Column column = schema.getColumn(columnName);

        if (column.getIndexType() != IndexTypes.NONE) {
            throw new OperationException(ErrorCode.ALREADY_INDEXED, "Cannot re-index an already indexed column. Execute drop-index before running the index command");
        }

        column.setIndexType(indexTypes);
        try {
            schemaManager.writeSchema(app, table, schema, true);
        } catch (JSONException ex) {

            //TODO: Notify admin
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "An internal operation error occurred.");
        }

        return indexRecords(app, table, columnName, operationLogLevel);
    }

    public String indexOffline(final String app, final String table, final String columnName, final IndexTypes indexTypes, final OperationLogLevel operationLogLevel) throws OperationException {
        SchemaManager schemaManager = new SchemaManager();
        Schema schema = schemaManager.readSchema(app, table);

        Column column = schema.getColumn(columnName);

        if (column.getIndexType() != IndexTypes.NONE) {
            throw new OperationException(ErrorCode.ALREADY_INDEXED, "Cannot re-index an already indexed column. Execute drop-index before running the index command");
        }

        column.setIndexType(indexTypes);
        try {
            schemaManager.writeSchema(app, table, schema, true);
        } catch (JSONException ex) {

            //TODO: Notify admin
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "An internal operation error occurred.");
        }

        return indexRecordsOffline(app, table, columnName, operationLogLevel);
    }

    /**
     * Passes all records in the database through an index operation for indexing only the specified column. This function creates an index operation file that
     * can be used to track the status of the indexing operation as the indexing operation is an asynchronous job.
     *
     * @param app The application id of the BlobCity application
     * @param table name of table within the specified application
     * @param columnName name of column within the specified table
     * @param logLevel the logging level for the indexing job. The job logs will be for the specified log level.
     * @return the operation id of the asynchronous index operation
     * @throws OperationException if an exception occurs. The {@link ErrorCode} within the exception will specify the type of error. This will only report
     * errors that prevent an index operation from starting. Errors post starting on the asynchronous indexing job will be reported in the operation log file.
     */
    public String indexRecords(final String app, final String table, final String columnName, OperationLogLevel logLevel) throws OperationException {
        Schema schema = schemaManager.readSchema(app, table);
        Column column = schema.getColumn(columnName);
        IndexingStrategy indexingStrategy = indexFactory.getStrategy(column.getIndexType());
        indexingStrategy.initializeIndexing(app, table, columnName);

        JSONObject jsonObject = new JSONObject();
        try {
            jsonObject = new JSONObject();
            jsonObject.put("type", OperationTypes.INDEX.getTypeCode());
            jsonObject.put("records", 0);
            jsonObject.put("time-started", -1);
            jsonObject.put("status", OperationStatus.NOT_STARTED.getStatusCode());
            jsonObject.put("column", columnName);
            jsonObject.put("index-type", column.getIndexType().getText());
        } catch (JSONException ex) {
            logger.error(null, ex);
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "Could not register long running indexing operation due to JSON error");
        }

        final String opid = operationsManager.registerOperation(app, table, OperationTypes.INDEX, jsonObject);
        logger.debug("Start indexing operation. App: {}, Table: {}, Column: {} with operation id: {}", new Object[]{app, table, columnName, opid});

        return opid;
    }

    public String indexRecordsOffline(final String app, final String table, final String columnName, OperationLogLevel logLevel) throws OperationException {
        SchemaManager schemaManager = new SchemaManager();
        Schema schema = schemaManager.readSchema(app, table);
        Column column = schema.getColumn(columnName);
        IndexFactory indexFactory = new IndexFactory();
        IndexingStrategy indexingStrategy = indexFactory.getStrategyInstance(column.getIndexType());
        indexingStrategy.initializeIndexing(app, table, columnName);

        JSONObject jsonObject = new JSONObject();
        try {
            jsonObject = new JSONObject();
            jsonObject.put("type", OperationTypes.INDEX.getTypeCode());
            jsonObject.put("records", 0);
            jsonObject.put("time-started", -1);
            jsonObject.put("status", OperationStatus.NOT_STARTED.getStatusCode());
            jsonObject.put("column", columnName);
            jsonObject.put("index-type", column.getIndexType().getText());
        } catch (JSONException ex) {
            logger.error(null, ex);
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "Could not register long running indexing operation due to JSON error");
        }

        OperationsManager operationsManager = new OperationsManager();
        final String opid = operationsManager.registerOperationForNextBoot(app, table, OperationTypes.INDEX, jsonObject);
        logger.debug("Will index App: {}, Table: {}, Column: {} with operation id: {}", new Object[]{app, table, columnName, opid});

        return opid;
    }

    public void dropIndex(final String app, final String table, final String columnName) throws OperationException {
        Schema schema = schemaManager.readSchema(app, table);

        Column column = schema.getColumn(columnName);

        if (schema.getPrimary().equals(column.getName())) {
            throw new OperationException(ErrorCode.PRIMARY_KEY_INDEX_DROP_RESTRICTED, "Cannot drop index of primary key");
        }

        if (column.getIndexType() == IndexTypes.NONE) {
            throw new OperationException(ErrorCode.NOT_INDEXED, "Column not indexed. No index to drop.");
        }

        column.setIndexType(IndexTypes.NONE);
        try {
            schemaManager.writeSchema(app, table, schema, true);
        } catch (JSONException ex) {

            //TODO: Notify admin
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "An internal operation error occurred.");
        }

        /* Move index folder to del location */

        final String absolutePath = PathUtil.indexColumnFolder(app, table, columnName);
        long currentTime = System.currentTimeMillis();
        String backupPath = BSql.BSQL_BASE_FOLDER + app + BSql.DELETE_FOLDER + "index-" + table + "." + columnName + "." + currentTime;
        try {
            int count = 0;
            while (Files.exists(FileSystems.getDefault().getPath(backupPath))) {
                backupPath = BSql.BSQL_BASE_FOLDER + app + BSql.DELETE_FOLDER + "index-" + table + "." + columnName + "." + currentTime + "_" + count;
                count++;
            }
            Files.move(FileSystems.getDefault().getPath(absolutePath), FileSystems.getDefault().getPath(backupPath), StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException ex) {

            //TODO: Notify Admin
            throw new OperationException(ErrorCode.COLLECTION_DELETION_ERROR, "Table could not be dropped as move to backup folder failed.");
        }
    }

    public void dropIndexOffline(final String app, final String table, final String columnName) throws OperationException {
        SchemaManager schemaManager = new SchemaManager();
        Schema schema = schemaManager.readSchema(app, table);

        Column column = schema.getColumn(columnName);

        if (schema.getPrimary().equals(column.getName())) {
            throw new OperationException(ErrorCode.PRIMARY_KEY_INDEX_DROP_RESTRICTED, "Cannot drop index of primary key");
        }

        if (column.getIndexType() == IndexTypes.NONE) {
            throw new OperationException(ErrorCode.NOT_INDEXED, "Column not indexed. No index to drop.");
        }

        column.setIndexType(IndexTypes.NONE);
        try {
            schemaManager.writeSchema(app, table, schema, true);
        } catch (JSONException ex) {

            //TODO: Notify admin
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "An internal operation error occurred.");
        }

        /* move index folder to del location */

        final String absolutePath = PathUtil.indexColumnFolder(app, table, columnName);
        long currentTime = System.currentTimeMillis();
        String backupPath = BSql.BSQL_BASE_FOLDER + app + BSql.DELETE_FOLDER + "index-" + table + "." + columnName + "." + currentTime;
        try {
            int count = 0;
            while (Files.exists(FileSystems.getDefault().getPath(backupPath))) {
                backupPath = BSql.BSQL_BASE_FOLDER + app + BSql.DELETE_FOLDER + "index-" + table + "." + columnName + "." + currentTime + "_" + count;
                count++;
            }
            Files.move(FileSystems.getDefault().getPath(absolutePath), FileSystems.getDefault().getPath(backupPath), StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException ex) {

            //TODO: Notify Admin
            throw new OperationException(ErrorCode.COLLECTION_DELETION_ERROR, "Table could not be dropped as move to backup folder failed.");
        }
    }

    /**
     * Indexes all columns that have an initializeIndexing type other than {@link IndexTypes.NONE}. None indexable columns are ignored. The primary key column
     * is also ignored.
     *
     * @param app The application id of the BlobCity application
     * @param table Name of table within the specified application
     * @param pk The primary key of the record to be indexed
     * @param jsonObject The JSONObject representing the complete record associated with the primary key. The JSONObject must be keyed on viewable column names.
     * @throws OperationException if an operation error occurs
     */
    public void addIndex(final String app, final String table, final String pk, JSONObject jsonObject) throws OperationException {
        final Schema schema = schemaManager.readSchema(app, table);
        if (schema.isIndexingNeeded()) {

            //new code start
            schema.getColumnMap().values().parallelStream().forEach(column -> {
                if (column.getName().equals(schema.getPrimary())) {
                    return;
                }

                if (column.getIndexType() != IndexTypes.NONE) {
                    try {
                        indexFactory.getStrategy(column.getIndexType()).index(app, table, column.getName(), jsonObject.get(column.getName()).toString(), pk);
                    } catch (JSONException | OperationException ex) {
                        //do nothing
                    }
                }
            });
            //new code end

            //TODO: Remove this code if index works perfectly with new implementation
//            for (Column column : schema.getColumnMap().values()) {
//                if (column.getName().equals(schema.getPrimary())) {
//                    continue;
//                }
//
//                if (column.getIndexType() != IndexTypes.NONE) {
//                    indexingStrategy = indexFactory.getStrategy(column.getIndexType());
//                    try {
//                        indexingStrategy.index(app, table, column.getName(), jsonObject.get(column.getName()).toString(), pk);
//                    } catch (JSONException ex) {
//
//                        //TODO: Notify admin
//                        logger.error(null, ex);
//                        throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "An internal operation error occurred. Unable to add index value for column: " + column.getName() + " in table: " + table);
//                    }
//                }
//            }
        }
    }

    /**
     * Re-indexes column values of columns that have changed. All keys in json objects have to viewableColumn names.
     *
     * @param app The application id of the BlobCity application
     * @param table Name of table within the specified application
     * @param pk
     * @param oldValues
     * @param newValues
     */
    public void diffIndex(final String app, final String table, final String pk, JSONObject oldValues, JSONObject newValues) throws OperationException {
        Schema schema = schemaManager.readSchema(app, table);
        IndexingStrategy indexingStrategy;
        if (schema.isIndexingNeeded()) {
            for (Column column : schema.getColumnMap().values()) {
                if (column.getName().equals(schema.getPrimary())) {
                    continue;
                }

                try {
                    if (!oldValues.has(column.getName())) {
                        /* If no old value to delete simply initializeIndexing the new value */
                        indexingStrategy = indexFactory.getStrategy(column.getIndexType());
                        indexingStrategy.index(app, table, column.getName(), newValues.get(column.getName()).toString(), pk);
                    } else if (oldValues.get(column.getName()).toString().equals(newValues.get(column.getName()).toString())) {
                        /* If old and new values are same, no re-indexing is required */
                        continue;
                    } else {
                        indexingStrategy = indexFactory.getStrategy(column.getIndexType());
                        indexingStrategy.remove(app, table, column.getName(), oldValues.get(column.getName()).toString(), pk);
                        indexingStrategy.index(app, table, column.getName(), newValues.get(column.getName()).toString(), pk);
                    }
                } catch (JSONException ex) {

                    //TODO: Notify admin
                    logger.error(null, ex);
                    throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "An internal operation error occurred. Unable index column: " + column + " in table: " + table);
                }
            }
        }
    }

    public void removeIndex(final String app, final String table, final String pk, final JSONObject jsonObject) throws OperationException {
        Schema schema = schemaManager.readSchema(app, table);
        if (schema.isIndexingNeeded()) {

            schema.getColumnMap().values().parallelStream().forEach(column -> {
                if (column.getName().equals(schema.getPrimary())) {
                    return;
                }

                if (column.getIndexType() != IndexTypes.NONE) {
                    try {
                        indexFactory.getStrategy(column.getIndexType()).remove(app, table, column.getName(), jsonObject.get(column.getName()).toString(), pk);
                    } catch (JSONException | OperationException ex) {
                        //do nothing
                    }
                }
            });

//            for (Column column : schema.getColumnMap().values()) {
//                if (column.getName().equals(schema.getPrimary())) {
//                    continue;
//                }
//
//                if (column.getIndexType() != IndexTypes.NONE) {
//                    indexingStrategy = indexFactory.getStrategy(column.getIndexType());
//                    try {
//                        indexingStrategy.remove(app, table, column.getName(), jsonObject.get(column.getName()).toString(), pk);
//                    } catch (JSONException ex) {
//                        //do nothing
//                    }
//                }
//            }
        }
    }

    public Iterator<String> readIndexStream(final String app, final String table, final String columnName, final Object columnValue) throws OperationException {
        Schema schema = schemaStore.getSchema(app, table);
        Column column = schema.getColumn(columnName);
        IndexingStrategy strategy = indexFactory.getStrategy(column.getIndexType());
        if(strategy == null) {
            final String opid = index(app, table, columnName, IndexTypes.BTREE, OperationLogLevel.ERROR);

            /**
             * The below semaphore mechanism is required in future when the operation is started in async manner
             */
//            Semaphore semaphore = new Semaphore(1);
//            semaphore.acquireUninterruptibly();
//            globalLiveStore.registerNotification(opid, semaphore);
//            semaphore.acquireUninterruptibly();

            column = schemaStore.getSchema(app, table).getColumn(columnName);
            strategy = indexFactory.getStrategy(column.getIndexType());
        }
        return strategy.loadIndexStream(app, table, columnName, columnValue.toString());
    }

    public long getIndexCount(final String app, final String table, final String columnName, final Object columnValue) throws OperationException {
        Schema schema = schemaStore.getSchema(app, table);
        Column column = schema.getColumn(columnName);
        IndexingStrategy strategy = indexFactory.getStrategy(column.getIndexType());
        if(strategy == null) {
            final String opid = index(app, table, columnName, IndexTypes.BTREE, OperationLogLevel.ERROR);

            /**
             * The below semaphore mechanism is required in future when the operation is started in async manner
             */
//            Semaphore semaphore = new Semaphore(1);
//            semaphore.acquireUninterruptibly();
//            globalLiveStore.registerNotification(opid, semaphore);
//            semaphore.acquireUninterruptibly();

            column = schemaStore.getSchema(app, table).getColumn(columnName);
            strategy = indexFactory.getStrategy(column.getIndexType());
        }
        return strategy.getIndexCount(app, table, columnName, columnValue.toString());
    }

    public Iterator<String> readIndexStreamWithFilter(final String app, final String table, final String columnName, final OperatorFileFilter filter) throws OperationException {
        Schema schema = schemaStore.getSchema(app, table);
        Column column = schema.getColumn(columnName);
        IndexingStrategy strategy = indexFactory.getStrategy(column.getIndexType());
        if(strategy == null) {
            final String opid = index(app, table, columnName, IndexTypes.BTREE, OperationLogLevel.ERROR);

            /**
             * The below semaphore mechanism is required in future when the operation is started in async manner
             */
//            Semaphore semaphore = new Semaphore(1);
//            semaphore.acquireUninterruptibly();
//            globalLiveStore.registerNotification(opid, semaphore);
//            semaphore.acquireUninterruptibly();

            column = schemaStore.getSchema(app, table).getColumn(columnName);
            strategy = indexFactory.getStrategy(column.getIndexType());
        }
        return strategy.loadIndexStream(app, table, columnName, filter);
    }

    public Iterator<String> getCardinals(final String ds, final String collection, final String columnName) throws OperationException {
        Schema schema = schemaStore.getSchema(ds, collection);
        Column column = schema.getColumn(columnName);
        if(column == null) {
            throw new OperationException(ErrorCode.COLUMN_INVALID, "No column found with name: " + columnName);
        }
        IndexingStrategy strategy = indexFactory.getStrategy(column.getIndexType());
        if(strategy == null) {
            final String opid = index(ds, collection, columnName, IndexTypes.BTREE, OperationLogLevel.ERROR);

            /**
             * The below semaphore mechanism is required in future when the operation is started in async manner
             */
//            Semaphore semaphore = new Semaphore(1);
//            semaphore.acquireUninterruptibly();
//            globalLiveStore.registerNotification(opid, semaphore);
//            semaphore.acquireUninterruptibly();

            column = schemaStore.getSchema(ds, collection).getColumn(columnName);
            strategy = indexFactory.getStrategy(column.getIndexType());
        }
        return strategy.cardinality(ds, collection, columnName);
    }

    public Iterator<String> getCardinalsTest(final String ds, final String collection, final String  columnName) throws OperationException {
        logger.debug("test pass");
        return getCardinals(ds, collection,  columnName);
    }

    public boolean contains(final String app, final String table, final String columnName, final Object columnValue, final Object pk) throws OperationException {
        Schema schema = schemaStore.getSchema(app, table);
        Column column = schema.getColumn(columnName);
        IndexingStrategy strategy = indexFactory.getStrategy(column.getIndexType());
        if(strategy == null) {
            final String opid = index(app, table, columnName, IndexTypes.BTREE, OperationLogLevel.ERROR);

            /**
             * The below semaphore mechanism is required in future when the operation is started in async manner
             */
//            Semaphore semaphore = new Semaphore(1);
//            semaphore.acquireUninterruptibly();
//            globalLiveStore.registerNotification(opid, semaphore);
//            semaphore.acquireUninterruptibly();

            column = schemaStore.getSchema(app, table).getColumn(columnName);
            strategy = indexFactory.getStrategy(column.getIndexType());
        }
        return strategy.contains(app, table, columnName, columnValue.toString(), pk.toString());
    }

    public String getAnyCardinalEntry(final String ds, final String collection, final String columnName, final Object columnValue) throws OperationException {
        Schema schema = schemaStore.getSchema(ds, collection);
        Column column = schema.getColumn(columnName);
        IndexingStrategy strategy = indexFactory.getStrategy(column.getIndexType());
        if(strategy == null) {
            final String opid = index(ds, collection, columnName, IndexTypes.BTREE, OperationLogLevel.ERROR);

            /**
             * The below semaphore mechanism is required in future when the operation is started in async manner
             */
//            Semaphore semaphore = new Semaphore(1);
//            semaphore.acquireUninterruptibly();
//            globalLiveStore.registerNotification(opid, semaphore);
//            semaphore.acquireUninterruptibly();

            column = schemaStore.getSchema(ds, collection).getColumn(columnName);
            strategy = indexFactory.getStrategy(column.getIndexType());
        }
        return strategy.getAnyCardinalEntry(ds, collection, columnName, columnValue.toString());
    }
}
