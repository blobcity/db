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

package com.blobcity.db.importer;

import au.com.bytecode.opencsv.CSVReader;
import com.blobcity.db.bquery.BQueryExecutorBean;
import com.blobcity.db.bquery.InternalQueryBean;
import com.blobcity.db.constants.BSql;
import com.blobcity.db.exceptions.ErrorCode;
import com.blobcity.db.exceptions.OperationException;
import com.blobcity.db.operations.Operable;
import com.blobcity.db.operations.OperationLogLevel;
import com.blobcity.db.operations.OperationLogger;
import com.blobcity.db.operations.OperationProperties;
import com.blobcity.db.operations.OperationStatus;
import com.blobcity.db.operations.OperationTypes;
import com.blobcity.db.operations.OperationsFileStore;
import com.blobcity.db.operations.OperationsManager;

import java.io.*;
import java.util.*;
import java.util.concurrent.Future;

import com.blobcity.db.sql.util.PathUtil;
import com.blobcity.db.util.Performance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.lang3.concurrent.ConcurrentUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.context.annotation.Lazy;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 *
 * @author sanketsarang
 */
@Component
public class CsvImporter implements Operable {

    private static final Logger logger = LoggerFactory.getLogger(CsvImporter.class);

    @Autowired
    private BQueryExecutorBean bQueryExecutor;
    @Autowired
    private OperationsFileStore operationsFileStore;
    @Autowired
    private OperationLogger operationLogger;
    @Autowired
    @Lazy
    private OperationsManager operationsManager;
    @Autowired
    @Lazy
    private InternalQueryBean internalQueryBean;

    @Override
    @Async
    public Future<OperationStatus> start(final String app, final String table, final String opid) throws OperationException {
        return start(app, table, opid, OperationLogLevel.ERROR);
    }

    @Override
    @Async
    public Future<OperationStatus> start(final String app, final String table, final String opid, final OperationLogLevel logLevel) throws OperationException {
        Map<String, String> columnMapping;
        String importFileLocation;
        JSONObject jsonObject;
        operationsFileStore.load(app, table, opid);
        jsonObject = operationsFileStore.getAsJson(opid);

        try {
            /* Import file location */
            String relativePath = jsonObject.getString("file");
            importFileLocation = PathUtil.datastoreFtpFolder(app) + (relativePath.startsWith(BSql.SEPERATOR) ? "" : BSql.SEPERATOR) + relativePath;

            logger.trace("Importing file: " + importFileLocation);

            /* Load column mapping */
            columnMapping = new HashMap<>();

            if(jsonObject.has("column-mapping")) {
                JSONObject columnMappingJson = jsonObject.getJSONObject("column-mapping");
                Iterator<String> iterator = columnMappingJson.keys();
                while (iterator.hasNext()) {
                    final String key = iterator.next();
                    columnMapping.put(key, columnMappingJson.getString(key));
                }
            } else {
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(importFileLocation)))) {
                    String headerRow = reader.readLine();

                    String []columns = headerRow.split(",");
                    Arrays.asList(columns).forEach(column -> columnMapping.put(column, "STRING"));
                } catch (IOException ex) {
                    throw new OperationException(ErrorCode.IMPORT_FILE_HEADER_MISSING);
                }
            }

        } catch (JSONException ex) {
            LoggerFactory.getLogger(CsvImporter.class.getName()).error(null, ex);
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR);
        }

        /* Start the operation */
        operationLogger.start(app, table, opid, logLevel);
        updateField(opid, OperationProperties.STATUS, OperationStatus.RUNNING.getStatusCode());
        updateField(opid, OperationProperties.TIME_STARTED, System.currentTimeMillis());
        updateField(opid, OperationProperties.TIME_STOPPED, -1);
        return run(app, table, opid, importFileLocation, columnMapping);
    }

    @Override
    public void stop(final String app, final String table, final String opid) throws OperationException {
        updateField(opid, OperationProperties.STATUS, OperationStatus.STOPPED.getStatusCode());
        updateField(opid, OperationProperties.TIME_STOPPED, System.currentTimeMillis());
        operationsFileStore.unload(opid);
        operationLogger.stop(opid);
        operationsManager.notifyComplete(opid);
    }

    private void onError(final String opid) {
        try {
            updateField(opid, OperationProperties.STATUS, OperationStatus.ERROR.getStatusCode());
            updateField(opid, OperationProperties.TIME_STOPPED, System.currentTimeMillis());
            operationsFileStore.unload(opid);
            operationLogger.stop(opid);
            operationsManager.notifyComplete(opid);
        } catch (OperationException ex) {
            LoggerFactory.getLogger(CsvImporter.class.getName()).error(null, ex);
        }
    }

    private void onComplete(final String opid) {
        try {
            updateField(opid, OperationProperties.STATUS, OperationStatus.COMPLETED.getStatusCode());
            updateField(opid, OperationProperties.TIME_STOPPED, System.currentTimeMillis());
            operationsFileStore.unload(opid);
            operationLogger.stop(opid);
            operationsManager.notifyComplete(opid);
        } catch (OperationException ex) {
            LoggerFactory.getLogger(CsvImporter.class.getName()).error(null, ex);
        }
    }

    public void updateField(final String opid, final String key, final Object value) throws OperationException {
        operationsFileStore.update(opid, key, value, true);
    }

    @Async
    private Future<OperationStatus> run(final String app, final String table, final String opid, final String fileLocation, final Map<String, String> columnMapping) {

        logger.trace("CsvImporter.run(ds={}, collection={}, opid={}, fileLocation={}, Map<String, String> columnMap)", app, table, opid, fileLocation);

        //TODO: There is clearly some problem with columnMapping. Need to figure out.

        long count = 0;
        try (CSVReader reader = new CSVReader(new FileReader(fileLocation))) {
            String[] items;

            /* Read first row containing column names */
            final String[] columnOrder = reader.readNext();

            List<Object> jsonRecords = new ArrayList<>();
            while (!Thread.interrupted()) {
                items = reader.readNext();

                if (items == null) {
                    /* import the last batch */
                    if(!jsonRecords.isEmpty()) {
                        logger.trace("Inserting " + jsonRecords.size() + " records into " + app + "." + table);
                        internalQueryBean.insertJSON(app, table, jsonRecords);
                    }

                    logger.trace("Completed Run: CsvImporter (ds={}, collection={}, opid={}, fileLocation={}, Map<String, String> columnMap)", app, table, opid, fileLocation);
                    onComplete(opid);
                    return ConcurrentUtils.constantFuture(OperationStatus.COMPLETED);
                }

                int index = 0;
                final int maxIndex = columnOrder.length;
                JSONObject requestJson = new JSONObject();
                try {
                    for (String item : items) {
                        if(index < maxIndex) {
                            requestJson.put(columnOrder[index++], item);
                        } else {
                            logger.debug("Ignoring column at " + index + " from CSV import for item: " + item);
                        }
                    }

                    try {
                        operationLogger.delayedLog(OperationLogLevel.FINE, opid, "Importing: " + Arrays.toString(items));
                    } catch (OperationException ex) {
                        LoggerFactory.getLogger(CsvImporter.class.getName()).error(null, ex);
                    }

                    jsonRecords.add(requestJson);
                    logger.trace("CsvImport will insert: " + requestJson.toString());

                    try {
                        operationLogger.delayedLog(OperationLogLevel.INFO, opid, "Imported: " + Arrays.toString(items));
                    } catch (OperationException ex) {
                        LoggerFactory.getLogger(CsvImporter.class.getName()).error(null, ex);
                    }

                    count++;

                    /* Fire the actual import in batches of 10000 */
                    if(count % Performance.PROCESSING_BATCH == 0) {
                        logger.trace("Inserting " + jsonRecords.size() + " records into " + app + "." + table);
                        internalQueryBean.insertJSON(app, table, jsonRecords);
                        jsonRecords = new ArrayList<>();
                    }

                    try {
                        operationsFileStore.update(opid, OperationProperties.RECORDS, count, false);
                    } catch (OperationException ex) {
                        LoggerFactory.getLogger(CsvImporter.class.getName()).error(null, ex);
                    }
                } catch (JSONException ex) {
                    logger.error("CsvImport (" + opid + ") will fail", ex);
                    onError(opid);
                    return ConcurrentUtils.constantFuture(OperationStatus.ERROR);
                }
            }
        } catch (FileNotFoundException ex) {
            LoggerFactory.getLogger(CsvImporter.class.getName()).error(null, ex);
            return ConcurrentUtils.constantFuture(OperationStatus.ERROR);
        } catch (IOException ex) {
            LoggerFactory.getLogger(CsvImporter.class.getName()).error(null, ex);
            return ConcurrentUtils.constantFuture(OperationStatus.ERROR);
        }

        return ConcurrentUtils.constantFuture(OperationStatus.STOPPED);
    }

    @Override
    public OperationTypes getType() {
        return OperationTypes.IMPORT;
    }
}
