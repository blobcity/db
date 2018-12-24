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

package com.blobcity.db.indexing;

import com.blobcity.db.bsql.BSqlDataManager;
import com.blobcity.db.exceptions.ErrorCode;
import com.blobcity.db.exceptions.OperationException;
import com.blobcity.db.operations.*;
import com.blobcity.db.schema.IndexTypes;
import org.apache.commons.lang3.concurrent.ConcurrentUtils;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.stereotype.Component;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Future;

/**
 *
 * @author sanketsarang
 */
@Component
public class IndexOperation implements Operable {
    public static final Logger logger = LoggerFactory.getLogger(IndexOperation.class);

    @Autowired
    private BSqlDataManager dataManager;
    @Autowired
    private OperationLogger operationLogger;
    @Autowired
    private OperationsFileStore operationsFileStore;
    @Autowired
    private IndexFactory indexFactory;
    @Autowired
    @Lazy
    private OperationsManager operationsManager;

    @Override
    public OperationTypes getType() {
        return OperationTypes.INDEX;
    }

    @Override
    public Future<OperationStatus> start(final String app, final String table, final String opid) throws OperationException {
        return start(app, table, opid, OperationLogLevel.ERROR);
    }

    @Override
    public Future<OperationStatus> start(final String app, final String table, final String opid, OperationLogLevel logLevel) throws OperationException {
        String column;
        IndexTypes indexType;
        operationsFileStore.load(app, table, opid);

        JSONObject jsonObject = operationsFileStore.getAsJson(opid);
        try {
            column = jsonObject.getString("column");
            indexType = IndexTypes.fromString(jsonObject.getString("index-type"));
        } catch (JSONException ex) {
            logger.error(null, ex);
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR);
        }

        operationLogger.start(app, table, opid, logLevel);
        updateField(opid, OperationProperties.STATUS, OperationStatus.RUNNING.getStatusCode());
        updateField(opid, OperationProperties.TIME_STARTED, System.currentTimeMillis());
        updateField(opid, OperationProperties.TIME_STOPPED, -1);
        return run(app, table, opid, column, indexType);
    }

    @Override
    public void stop(final String app, final String table, final String opid) throws OperationException {
        updateField(opid, "status", OperationStatus.STOPPED.getStatusCode());
        updateField(opid, OperationProperties.TIME_STOPPED, System.currentTimeMillis());
        operationsFileStore.unload(opid);
        operationLogger.stop(opid);
        operationsManager.notifyComplete(opid);
    }

    private void onError(final String opid) {
        try {
            updateField(opid, "status", OperationStatus.ERROR.getStatusCode());
            updateField(opid, OperationProperties.TIME_STOPPED, System.currentTimeMillis());
            operationsFileStore.unload(opid);
            operationLogger.stop(opid);
            operationsManager.notifyComplete(opid);
        } catch (OperationException ex) {
            logger.error(null, ex);
        }
    }

    private void onComplete(final String opid) {
        try {
            updateField(opid, "status", OperationStatus.COMPLETED.getStatusCode());
            updateField(opid, OperationProperties.TIME_STOPPED, System.currentTimeMillis());
            operationsFileStore.unload(opid);
            operationLogger.stop(opid);
            operationsManager.notifyComplete(opid);
        } catch (OperationException ex) {
            logger.error(null, ex);
        }
    }

    private void updateField(final String opid, String key, Object value) throws OperationException {
        operationsFileStore.update(opid, key, value, true);
    }

    @Async
    private Future<OperationStatus> run(final String app, final String table, final String opid, final String column, final IndexTypes indexType) {
        return run(app, table, opid, column, indexType, false);
    }

    /**
     * TEMPORARY IMPLEMENTATION - Should be replaced with the original DirectoryStream function
     */
    @Async
    private Future<OperationStatus> run(final String app, final String table, final String opid, final String column, final IndexTypes indexType, boolean inMemory) {
        List<String> keysList;
        IndexingStrategy indexingStrategy = indexFactory.getStrategy(indexType);
        try {
            keysList = dataManager.selectAllKeys(app, table);
        } catch (OperationException ex) {
            logger.error(null, ex);
            onError(opid);
            return new AsyncResult<>(OperationStatus.ERROR);
        }

        keysList.parallelStream().forEach(key -> {
            try {
                JSONObject jsonObject = dataManager.select(app, table, key);
                Object value = jsonObject.get(column);
                operationLogger.delayedLog(OperationLogLevel.FINE, opid, "Indexing: " + key);
                indexingStrategy.index(app, table, column, value.toString(), key);
                operationLogger.delayedLog(OperationLogLevel.INFO, opid, "Indexed: " + key);
            } catch (OperationException | JSONException ex) {
                try {
                    operationLogger.delayedLog(OperationLogLevel.ERROR, opid, ex.getMessage());
                } catch (OperationException ex1) {
                    logger.error(null, ex1);
                }
            }
        });

        try {
            operationsFileStore.update(opid, OperationProperties.RECORDS, keysList.size(), false);
        } catch (OperationException e) {
            logger.error(null, e);
        }

        onComplete(opid);
        return ConcurrentUtils.constantFuture(OperationStatus.COMPLETED);
    }


    /**
     * ORIGINAL IMPLEMENTATION: Uses the recommended iterator to read the keys. The DirectoryStream for reading all keys
     * in the table is failing for unknown reasons. Replacing with select all keys.
     * @param app
     * @param table
     * @param opid
     * @param column
     * @param indexType
     * @param inMemory
     * @return
     */
    @Async
    private Future<OperationStatus> runOld(final String app, final String table, final String opid, final String column, final IndexTypes indexType, boolean inMemory) {
        Iterator<String> iterator;
        IndexingStrategy indexingStrategy = indexFactory.getStrategy(indexType);

        try {
            iterator = dataManager.selectAllKeysAsStream(app, table);
        } catch (OperationException ex) {
            logger.error(null, ex);
            onError(opid);
            return new AsyncResult<>(OperationStatus.ERROR);
        }

        long count = 0;
        while (!Thread.interrupted() && iterator.hasNext()) {
            final String key = iterator.next();
            try {
                JSONObject jsonObject = dataManager.select(app, table, key);
                Object value = jsonObject.get(column);
                operationLogger.delayedLog(OperationLogLevel.FINE, opid, "Indexing: " + key);
                indexingStrategy.index(app, table, column, value.toString(), key);
                operationLogger.delayedLog(OperationLogLevel.INFO, opid, "Indexed: " + key);
                count++;
                operationsFileStore.update(opid, OperationProperties.RECORDS, count, false);
            } catch (OperationException | JSONException ex) {
                try {
                    operationLogger.delayedLog(OperationLogLevel.ERROR, opid, ex.getMessage());
                } catch (OperationException ex1) {
                    logger.error(null, ex1);
                }
            }
        }

        onComplete(opid);
        return ConcurrentUtils.constantFuture(OperationStatus.COMPLETED);
    }
}
