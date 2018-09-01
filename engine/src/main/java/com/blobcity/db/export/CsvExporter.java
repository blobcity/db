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

package com.blobcity.db.export;

import au.com.bytecode.opencsv.CSVWriter;
import com.blobcity.db.bsql.BSqlDataManager;
import com.blobcity.db.exceptions.ErrorCode;
import com.blobcity.db.exceptions.OperationException;
import com.blobcity.db.importer.CsvImporter;
import com.blobcity.db.operations.*;
import com.blobcity.db.schema.Schema;
import com.blobcity.db.schema.beans.SchemaStore;
import com.blobcity.db.sql.util.PathUtil;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.stereotype.Component;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Future;

/**
 *
 * @author sanketsarang
 */
@Component("CsvExporter")
public class CsvExporter implements Operable {

    public static final Logger logger = LoggerFactory.getLogger(CsvExporter.class);

    @Autowired
    private OperationsFileStore operationsFileStore;
    @Autowired
    private BSqlDataManager dataManager;
    @Autowired
    private SchemaStore schemaStore;
    @Autowired
    private OperationLogger operationLogger;

    @Autowired
    @Lazy
    private OperationsManager operationsManager;

    @Override
    @Async
    public Future<OperationStatus> start(String app, String table, String opid) throws OperationException {
        logger.info("Registered operation for opId " + opid);
        return start(app, table, opid, OperationLogLevel.ERROR);
    }

    @Override
    @Async
    public Future<OperationStatus> start(String app, String table, String opid, OperationLogLevel logLevel) throws OperationException {
        String exportFileLocation;
        JSONObject jsonObject;
        operationsFileStore.load(app, table, opid);
        jsonObject = operationsFileStore.getAsJson(opid);

        try {

            /* Export file location */
            exportFileLocation = PathUtil.exportFile(app, jsonObject.getString("file"));
        } catch (JSONException ex) {
            LoggerFactory.getLogger(CsvImporter.class.getName()).error(null, ex);
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR);
        }

        operationLogger.start(app, table, opid, logLevel);
        updateField(opid, OperationProperties.STATUS, OperationStatus.RUNNING.getStatusCode());
        updateField(opid, OperationProperties.TIME_STARTED, System.currentTimeMillis());
        updateField(opid, OperationProperties.TIME_STOPPED, -1);
        return run(app, table, opid, exportFileLocation);
    }

    @Override
    public void stop(String app, String table, String opid) throws OperationException {
        updateField(opid, OperationProperties.STATUS, OperationStatus.STOPPED.getStatusCode());
        updateField(opid, OperationProperties.TIME_STOPPED, System.currentTimeMillis());
        operationsFileStore.unload(opid);
        operationLogger.stop(opid);
        operationsManager.notifyComplete(opid);
    }

    @Override
    public OperationTypes getType() {
        return OperationTypes.EXPORT;
    }

    private void onComplete(final String opid) {
        try {
            updateField(opid, OperationProperties.STATUS, OperationStatus.COMPLETED.getStatusCode());
            updateField(opid, OperationProperties.TIME_STOPPED, System.currentTimeMillis());
            operationsFileStore.unload(opid);
            operationLogger.stop(opid);
            operationsManager.notifyComplete(opid);
        } catch (OperationException ex) {
            logger.error(null, ex);
        }
    }

    private void onError(final String opid) {
        try {
            updateField(opid, OperationProperties.STATUS, OperationStatus.ERROR.getStatusCode());
            updateField(opid, OperationProperties.TIME_STOPPED, System.currentTimeMillis());
            operationsFileStore.unload(opid);
            operationLogger.stop(opid);
            operationsManager.notifyComplete(opid);
        } catch (OperationException ex) {
            logger.error(null, ex);
        }
    }

    public void updateField(final String opid, final String key, final Object value) throws OperationException {
        operationsFileStore.update(opid, key, value, true);
    }

    private Future<OperationStatus> run(final String app, final String table, final String opid, final String exportFilePath) {
        return run(app, table, opid, exportFilePath, false);
    }
    
    private Future<OperationStatus> run(final String app, final String table, final String opid, final String exportFilePath, boolean inMemory) {
        String key;
        Schema schema;
        List<String> columnOrder = new ArrayList<>();
        List<String> values;
        File file = new File(exportFilePath);

        /* Read and proces schema */
        try {
            schema = schemaStore.getSchema(app, table);

            /* Make primary key first column */
            columnOrder.add(schema.getPrimary());

            /* Add all other columns in any arbitrary order */
            for (String columnName : schema.getColumnMap().keySet()) {
                if (!columnOrder.contains(columnName)) {
                    columnOrder.add(columnName);
                }
            }
        } catch (OperationException ex) {
            try {
                operationLogger.delayedLog(OperationLogLevel.ERROR, opid, ex.getMessage());
            } catch (OperationException ex1) {
                logger.error(null, ex);
            }
            onError(opid);
            return new AsyncResult<>(OperationStatus.ERROR);
        }

        try (CSVWriter writer = new CSVWriter(new FileWriter(file))) {

            /* Write file line for column names */
            writer.writeNext(columnOrder.toArray(new String[columnOrder.size()]));

            /* Iterate through all records in the table */
            long count = 0;
            Iterator<String> iterator = dataManager.selectAllKeysAsStream(app, table);
            List<JSONObject> dataCache = new ArrayList<>(10000);
            values = new ArrayList<>(columnOrder.size());
            while (iterator.hasNext()) {
                key = iterator.next();
                try {
                    dataCache.add(dataManager.select(app, table, key));
                } catch (OperationException ex) {
                    operationLogger.delayedLog(OperationLogLevel.ERROR, opid, "Record with pk " + key
                            + " could not be exported due to the following cause:");
                    logException(OperationLogLevel.ERROR, opid, ex);
                    continue;
                }

                /* Periodically dump data to file */
                if (dataCache.size() == 10000) {
                    for (JSONObject jsonObject : dataCache) {
                        /* Populate values array */
                        values.clear();
                        for (String column : columnOrder) {
                            if (jsonObject.has(column)) {
                                values.add(jsonObject.get(column).toString());
                            } else {
                                values.add("");
                            }
                        }

                        writer.writeNext(values.toArray(new String[values.size()]));
                        count++;
                        operationsFileStore.update(opid, OperationProperties.RECORDS, count, false);
                    }
                    dataCache.clear();
                }
            }

            /* dump any remaining cached records */
            for (JSONObject jsonObject : dataCache) {
                /* Populate values array */
                values = new ArrayList<>(columnOrder.size());
                for (String column : columnOrder) {
                    if (jsonObject.has(column)) {
                        values.add(jsonObject.get(column).toString());
                    } else {
                        values.add("");
                    }
                }

                writer.writeNext(values.toArray(new String[values.size()]));
                count++;
                operationsFileStore.update(opid, OperationProperties.RECORDS, count, false);
            }

            onComplete(opid);
            return new AsyncResult<>(OperationStatus.COMPLETED);
        } catch (IOException | OperationException | JSONException ex) {
            logException(OperationLogLevel.ERROR, opid, ex);
            logger.error(null, ex);
        }
        try {
            operationLogger.delayedLog(OperationLogLevel.ERROR, opid, "The operation could not be successfully completed and is now stopping itself.");
        } catch (OperationException ex) {
            logger.error(null, ex);
        }
        onError(opid);
        return new AsyncResult<>(OperationStatus.ERROR);
    }

    private void logException(final OperationLogLevel operationLogLevel, final String opid, Exception ex) {
        try {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            operationLogger.delayedLog(OperationLogLevel.ERROR, opid, sw.getBuffer().toString());
        } catch (OperationException ex1) {
            logger.error(null, ex1);
        }
    }
}
