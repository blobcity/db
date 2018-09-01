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

package com.blobcity.db.operations;

import com.blobcity.db.exceptions.ErrorCode;
import com.blobcity.db.exceptions.OperationException;
import com.blobcity.db.export.ExportType;
import com.blobcity.db.global.live.GlobalLiveManager;
import com.blobcity.db.global.live.GlobalLiveStore;
import com.blobcity.db.importer.ImportType;
import com.blobcity.db.sql.util.PathUtil;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Future;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.context.annotation.Lazy;

/**
 *
 * @author sanketsarang
 */
@Component
public class OperationsManager {

    @Autowired
    private GlobalLiveManager globalLiveManager;
    @Autowired
    private OperationQueue operationQueue;
    @Autowired
    private ActiveOperationStore activeOperationStore;
    @Autowired
    @Lazy
    private GlobalLiveStore globalLiveStore;
    @Autowired
    private OperationExecutor operationExecutor;

    public String registerOperation(final String app, final String table, final OperationTypes operationType, final JSONObject jsonObject) throws OperationException {
        final String opid = registerOperation(app, table, operationType);
        try {
            jsonObject.put("log", PathUtil.operationLogFile(app, table, opid));
        } catch (JSONException ex) {
            LoggerFactory.getLogger(OperationsManager.class.getName()).error(null, ex);
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR);
        }
        registerGlobal(app, table, opid, operationType);
        writeOperationFile(app, table, opid, jsonObject.toString());
        operationQueue.enqueue(app, opid);
        globalLiveStore.add(app, opid);
        tryStartNext(app);
        return opid;
    }

    public String registerOperationForNextBoot(final String app, final String table, final OperationTypes operationType, final JSONObject jsonObject) throws OperationException {
        final String opid = registerOperation(app, table, operationType);
        try {
            jsonObject.put("log", PathUtil.operationLogFile(app, table, opid));
        } catch (JSONException ex) {
            LoggerFactory.getLogger(OperationsManager.class.getName()).error(null, ex);
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR);
        }
        writeOperationFile(app, table, opid, jsonObject.toString());
        return opid;
    }

    public void notifyComplete(final String opid) throws OperationException {
        final String app = globalLiveStore.getApp(opid);
        if (app == null) {
            LoggerFactory.getLogger(OperationsManager.class.getName()).error("Next operation wont start for app: "
                    + "{} as previous operation with opid: {1} failed to map to an application.", new Object[]{app, opid});
            return;
        }
        globalLiveStore.notifyComplete(opid);
        globalLiveStore.removeOperation(opid);
        activeOperationStore.remove(opid);
        globalLiveManager.remove(opid);
        tryStartNext(app);
    }

    public List<String> getOperations(final String app, final String table) {
        final File file = new File(PathUtil.operationFolder(app, table));
        final String[] fileNames = file.list((File dir, String name) -> (name.startsWith(OperationTypes.IMPORT.getTypeCode())
                || name.startsWith(OperationTypes.INDEX.getTypeCode())
                || name.startsWith(OperationTypes.EXPORT.getTypeCode()))
                && !name.endsWith("log"));

        if (fileNames == null) {
            return Collections.EMPTY_LIST;
        }

        return new ArrayList<>(Arrays.asList(fileNames));
    }

    public List<String> getOperations(final String app, final String table, final OperationTypes operationType) {
        final File file = new File(PathUtil.operationFolder(app, table));
        final String[] fileNames = file.list((File dir, String name) -> name.startsWith(operationType.getTypeCode()));

        return new ArrayList<>(Arrays.asList(fileNames));
    }

    public boolean containsOperation(final String app, final String table, final String opid) {
        return Files.exists(FileSystems.getDefault().getPath(PathUtil.operationFile(app, table, opid)));
    }

    public String readOperationFile(final String app, final String table, final String opid) throws OperationException {
        List<String> contents;

        if (!containsOperation(app, table, opid)) {
            throw new OperationException(ErrorCode.INEXISTENT_OPERATION);
        }

        try {
            contents = Files.readAllLines(FileSystems.getDefault().getPath(PathUtil.operationFile(app, table, opid)), Charset.forName("UTF8"));
        } catch (IOException ex) {
            LoggerFactory.getLogger(OperationsManager.class.getName()).error(null, ex);
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "Could not read operation file for op: " + opid + " in app: " + app + " for table: " + table);
        }

        if (contents == null || contents.isEmpty()) {
            return "";
        }

        if (contents.size() > 1) {
            LoggerFactory.getLogger(OperationsManager.class.getName()).warn("Some lines in operation file have been ignored for op: {} in app: {} for table: {}", new Object[]{opid, app, table});
        }

        return contents.get(0);
    }

    public void writeOperationFile(final String app, final String table, final String opid, final String contents) throws OperationException {
        try {
            Files.write(FileSystems.getDefault().getPath(PathUtil.operationFile(app, table, opid)), contents.getBytes());
        } catch (IOException ex) {
            LoggerFactory.getLogger(OperationsManager.class.getName()).error(null, ex);
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR);
        }
    }

    private String registerOperation(final String app, final String table, final OperationTypes operationType) throws OperationException {
        final String opid = operationType.getTypeCode() + UUID.randomUUID().toString();
        try {
            if (Files.exists(FileSystems.getDefault().getPath(PathUtil.operationFile(app, table, opid)))) {
                throw new OperationException(ErrorCode.DUPLICATE_OPERATION, "An operation with the specified opid: " + opid + " is already existent. Consider loading/resuming the existing operation instead.");
            }

            Files.createFile(FileSystems.getDefault().getPath(PathUtil.operationFile(app, table, opid)));
        } catch (IOException ex) {
            LoggerFactory.getLogger(OperationsManager.class.getName()).error(null, ex);
            throw new OperationException(ErrorCode.CANNOT_REGISTER_OPERATION, "Operation file could not be created for app: " + app + " in table: " + table);
        }
        return opid;
    }

    private void registerGlobal(final String app, final String table, final String opid, final OperationTypes operationType) throws OperationException {
        JSONObject jsonObject = new JSONObject();
        try {
            jsonObject.put("app", app);
            jsonObject.put("t", table);
            jsonObject.put("opid", opid);
            jsonObject.put("type", operationType.getTypeCode());
            globalLiveManager.register(opid, jsonObject);
        } catch (JSONException ex) {
            LoggerFactory.getLogger(OperationsManager.class.getName()).error(null, ex);
        }
    }

//    @Lock(LockType.WRITE)
    public void tryStartNext(final String app) throws OperationException {
        final String opid;
        final String type;
        final OperationTypes operationType;
        final String table;
        final JSONObject operationFileJson;
        final Future<OperationStatus> futureResponse;
        Set<String> ops = globalLiveStore.getOps(app);
        for (String op : ops) {
            if (activeOperationStore.contains(op)) {
                return;
            }
        }

        if (!operationQueue.hasNext(app)) {
            return;
        }

        opid = operationQueue.next(app);
        JSONObject jsonObject = globalLiveManager.get(opid);

        try {
            type = jsonObject.getString("type");
            operationType = OperationTypes.fromString(type);
            switch (operationType) {
                case IMPORT:
                    table = jsonObject.getString("t");
                    operationFileJson = new JSONObject(readOperationFile(app, table, opid));
                    final ImportType importType = ImportType.valueOf(operationFileJson.getString("import-type"));
                    futureResponse = operationExecutor.startOperation(app, table, opid, OperationTypes.IMPORT, new String[]{importType.getTypeCode()});
                    activeOperationStore.add(app, opid, futureResponse);
                    break;
                case INDEX:
                    table = jsonObject.getString("t");
                    futureResponse = operationExecutor.startOperation(app, table, opid, OperationTypes.INDEX, new String[]{});
                    activeOperationStore.add(app, opid, futureResponse);
                    break;
                case EXPORT:
                    table = jsonObject.getString("t");
                    operationFileJson = new JSONObject(readOperationFile(app, table, opid));
                    final ExportType exportType = ExportType.valueOf(operationFileJson.getString("export-type"));
                    futureResponse = operationExecutor.startOperation(app, table, opid, OperationTypes.EXPORT, new String[]{exportType.getTypeCode()});
                    activeOperationStore.add(app, opid, futureResponse);
                    break;
            }
        } catch (JSONException ex) {
            LoggerFactory.getLogger(OperationsManager.class.getName()).error(null, ex);
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR);
        }
    }
}
