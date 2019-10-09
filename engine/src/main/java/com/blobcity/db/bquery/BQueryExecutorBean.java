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

package com.blobcity.db.bquery;

import com.blobcity.db.bsql.BSqlCollectionManager;
import com.blobcity.db.bsql.BSqlDataManager;
import com.blobcity.db.bsql.BSqlDatastoreManager;
import com.blobcity.db.bsql.BSqlIndexManager;
import com.blobcity.db.bsql.ClusterDataManager;
import com.blobcity.db.ftp.FtpServiceManager;
import com.blobcity.db.master.MasterStore;
import com.blobcity.db.code.CodeExecutor;
import com.blobcity.db.code.CodeLoader;
import com.blobcity.db.code.ManifestParserBean;
import com.blobcity.db.constants.BQueryCommands;
import com.blobcity.db.constants.BQueryParameters;
import com.blobcity.db.constants.Governor;
import com.blobcity.db.exceptions.ErrorCode;
import com.blobcity.db.exceptions.OperationException;
import com.blobcity.db.lang.Operators;
import com.blobcity.db.lang.columntypes.FieldType;
import com.blobcity.db.lang.columntypes.FieldTypeFactory;
import com.blobcity.db.locks.MasterLockBean;
import com.blobcity.db.locks.TransactionLocking;
import com.blobcity.db.operations.OperationLogLevel;
import com.blobcity.db.operations.OperationTypes;
import com.blobcity.db.operations.OperationsManager;
import com.blobcity.db.requests.RequestHandlingBean;
import com.blobcity.db.schema.*;
import com.blobcity.db.schema.beans.SchemaStore;
import com.blobcity.db.security.SecurityManagerBean;
import com.blobcity.db.security.exceptions.BadPasswordException;
import com.blobcity.db.security.exceptions.BadUsernameException;
import com.blobcity.db.security.exceptions.InvalidCredentialsException;
import com.blobcity.db.sql.util.PathUtil;
import com.blobcity.db.storage.BSqlFileManager;
import com.blobcity.db.tableau.TableauPublishStore;
import com.blobcity.db.util.JSONOperationException;
import com.blobcity.lib.database.bean.manager.interfaces.engine.BQueryExecutor;
import com.blobcity.lib.database.bean.manager.interfaces.engine.QueryData;
import com.blobcity.lib.database.bean.manager.interfaces.engine.QueryStore;
import com.blobcity.lib.query.Query;
import com.blobcity.lib.query.QueryParams;
import com.blobcity.lib.query.RecordType;
import com.blobcity.util.json.JsonMessages;
import com.google.common.io.Files;
import com.google.gson.Gson;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

/**
 * Bean responsible for handling all the REST and Client Side Adapter queries
 *
 * @author sanketsarang
 * @author Prikshit Kumar
 */
@Component
public class BQueryExecutorBean implements BQueryExecutor {

    private static final Logger logger = LoggerFactory.getLogger(BQueryExecutorBean.class.getName());

    private int limit = Governor.SELECT_ALL_LIMIT;

    @Autowired(required = false)
    @Lazy
    private ClusterDataManager clusterDataManager;
    @Autowired
    private CodeExecutor codeExecutor;
    @Autowired(required = false)
    @Lazy
    private CodeLoader codeLoader;
    @Autowired(required = false)
    @Lazy
    private BSqlCollectionManager collectionManager;
    @Autowired(required = false)
    @Lazy
    private BSqlDataManager dataManager;
    @Autowired(required = false)
    BSqlDatastoreManager datastoreManager;
    @Autowired(required = false)
    @Lazy
    private BSqlFileManager fileManager;
    @Autowired(required = false)
    @Lazy
    private BSqlIndexManager indexManager;
    @Autowired(required = false)
    @Lazy
    private ManifestParserBean manifestParserBean;
    @Autowired(required = false)
    @Lazy
    private MasterLockBean masterLockBean;
    @Autowired(required = false)
    @Lazy
    private MasterStore masterStore;
    @Autowired(required = false)
    @Lazy
    private OperationsManager operationsManager;
    @Autowired
    private QueryStore requestStore;
    @Autowired
    private SecurityManagerBean securityManager;
    @Autowired
    private TransactionLocking transactionLocking;
    @Autowired
    private RequestHandlingBean requestHandlingBean;
    @Autowired
    private FtpServiceManager ftpServiceManager;
    @Lazy @Autowired
    private TableauPublishStore tableauPublishStore;

    @Override
    public String runQuery(final String jsonString) {
        BQueryCommands command = null;
        String ds = null;
        //TODO: App should eventually be picked up from the RequestStore
        String table = null;
        String requestId = null;
        JSONObject jsonObject;
        JSONObject payloadJson;
        JSONObject returnJson = null;

        logger.debug("Query: " + jsonString);

        try {
            jsonObject = new JSONObject(jsonString);
        } catch (JSONException ex) {
            return null;
        }
        try {
            try {
                /* Get datastore information */
                if(jsonObject.has(BQueryParameters.DATASTORE)) {
                    ds = jsonObject.getString(BQueryParameters.DATASTORE);
                }else if(jsonObject.has(BQueryParameters.DATABASE)) {
                    ds = jsonObject.getString(BQueryParameters.DATABASE);
                }else if(jsonObject.has(BQueryParameters.ACCOUNT)) {
                    ds = jsonObject.getString(BQueryParameters.ACCOUNT);
                }else {
                    ds = null;
                }

                // Requests with null 'app' value will also be registered.
                requestId = UUID.randomUUID().toString();
                requestStore.register(ds, requestId, new QueryData(jsonString, System.currentTimeMillis())); //this is required to pass parameters to stored procedures

                /* Checks if application is present on the node on which this code is executing. Do
                * not return any jms response until this check passes
                */
//            if (!applicationRequestBeanRemote.containsApplication(account)) {
//                return null;
//            }

                /* Set the collection property */
                if(jsonObject.has(BQueryParameters.TABLE)) {
                    table = jsonObject.getString(BQueryParameters.TABLE);
                } else if(jsonObject.has(BQueryParameters.COLLECTION)) {
                    table = jsonObject.getString(BQueryParameters.COLLECTION);
                } else {
                    table = null;
                }

                try {
                    if (jsonObject.getInt(BQueryParameters.LIMIT) < Governor.SELECT_ALL_LIMIT) {
                        limit = jsonObject.getInt(BQueryParameters.LIMIT);
                    }
                } catch (JSONException ex) {
                    limit = Governor.SELECT_ALL_LIMIT;
                }
                //requestIdentifier = jsonObject.getString(("rid"));
                command = BQueryCommands.fromString(jsonObject.getString(BQueryParameters.QUERY));
                if (command == null) {
                    throw new OperationException(ErrorCode.INVALID_QUERY, "The query: "
                            + jsonObject.getString(BQueryParameters.QUERY) + " could not be recognized as a valid query");
                }

                try {
                    payloadJson = jsonObject.getJSONObject(BQueryParameters.PAYLOAD);
                } catch (JSONException ex) {
                    payloadJson = null;
                }

            } catch (JSONException ex) {
                throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "Required parameters missing from query");
            }

            final long startTime = System.currentTimeMillis();
            switch (command) {

                /* Data */
                case INSERT:
//                    returnJson = clusterDataManager.insert(app, table, jsonObject.getJSONObject(BQueryParameters.PAYLOAD));
                    returnJson = insert(ds, table, jsonObject.getJSONObject(BQueryParameters.PAYLOAD));
                    break;
                case UPDATE:
                    returnJson = clusterDataManager.update(ds, table, jsonObject.getJSONObject(BQueryParameters.PAYLOAD));
                    break;
                case DELETE:
//                    returnJson = clusterDataManager.remove(jsonObject);
                    returnJson = delete(ds, table, jsonObject.getString("pk")); //for delete capability until migration to cluster
                    break;
                case INSERT_CUSTOM:
                    returnJson = insertViaInterpreter(ds, table, payloadJson);
                    break;
                case SELECT:
                    returnJson = clusterDataManager.select(jsonObject);
                    break;
                case BULK_SELECT:
                    returnJson = bulkSelect(ds, table, jsonObject.getJSONArray("pk"));
                    break;
                case CONTAINS:
                    returnJson = contains(ds, table, jsonObject.getString("pk"));
                    break;
                case SELECT_ALL:
                    returnJson = selectAll(ds, table);
                    break;
                case SAVE:
                    returnJson = save(ds, table, payloadJson);
                    break;

                /* Datastore and Collection */
                case CREATE_DB:
                case CREATE_DS:
                    returnJson = createDatastore(payloadJson);
                    break;
                case DROP_DB:
                case DROP_DS:
                    returnJson = dropDatastore(payloadJson);
                    break;
                case TRUNCATE_DS:
                    /* locking is managed within the function */
                    returnJson = truncateDs(payloadJson);
                    break;
                case LIST_DS:
                    returnJson = listDatastores();
                    break;
                case DS_EXISTS:
                    returnJson = dsExists(payloadJson);
                    break;
                case CREATE_TABLE:
                case CREATE_COLLECTION:
                    returnJson = createTable(ds, payloadJson);
                    break;
                case DROP_TABLE:
                case DROP_COLLECTION:
                    returnJson = dropTable(ds, table);
                    break;
                case RENAME_TABLE:
                    returnJson = renameTable(ds, table, payloadJson);
                    break;
                case TRUNCATE_TABLE:
                    returnJson = truncateTable(ds, table);
                    break;
                case LIST_TABLES:
                    returnJson = listTables(ds);
                    break;
                case LIST_COLLECTIONS:
                    returnJson = listCollections(payloadJson);
                    break;
                case VIEW_SCHEMA:
                    returnJson = listSchema(ds, table);
                    break;
                case TABLE_EXISTS:
                    returnJson = tableExists(ds, table);
                    break;
                case COLLECTION_EXISTS:
                    returnJson = collectionExists(payloadJson);
                    break;

                /* Column */
                case ADD_COLUMN:
                    returnJson = addColumn(ds, table, payloadJson);
                    break;
                case DROP_COLUMN:
                    returnJson = dropColumn(ds, table, payloadJson);
                    break;
                case RENAME_COLUMN:
                    returnJson = renameColumn(ds, table, payloadJson);
                    break;
                case ALTER_COLUMN:
                    returnJson = alterColumn(ds, table, payloadJson);
                    break;
                case CHANGE_DATA_TYPE:
                    returnJson = changeDataType(ds, table, jsonObject);
                    break;
                case DROP_UNIQUE:
                    break;
                case INDEX:
                    returnJson = index(ds, table, payloadJson);
                    break;
                case DROP_INDEX:
                    returnJson = dropIndex(ds, table, payloadJson);
                    break;
                case SET_AUTO_DEFINE:
                    returnJson = setAutoDefine(ds, table, payloadJson);
                    break;

                /* Search */
                case SEARCH:
                    returnJson = search(ds, payloadJson);
                    break;
                case SEARCH_AND:
                    returnJson = searchAnd(ds, table, payloadJson, false);
                    break;
                case SEARCH_OR:
                    returnJson = searchOr(ds, table, payloadJson, false);
                    break;
                case SEARCH_AND_LOAD:
                    returnJson = searchAnd(ds, table, payloadJson, true);
                    break;
                case SEARCH_OR_LOAD:
                    returnJson = searchOr(ds, table, payloadJson, true);
                    break;
                case BULK_IMPORT:
                    returnJson = bulkImport(ds, table, payloadJson);
                    break;

                /* FTP Service */
                case START_DS_FTP:
                    returnJson = startDsFtp(ds);
                    break;
                case STOP_DS_FTP:
                    returnJson = stopDsFtp(ds);
                    break;

                /* Management */
                case ADD_USER:
                    returnJson = createUser(payloadJson);
                    break;
                case DROP_USER:
                    returnJson = dropUser(payloadJson);
                    break;

                    /* Tableau */
                case TABLEAU_REQUIRES_SYNC:
                    returnJson = tableauRequiresSync();
                    break;

                /* User Code related */
                case LOAD_CODE:
                    returnJson = loadCode(ds, payloadJson);
                    break;
                case SP:
//                    throw new UnsupportedOperationException("Not supported yet in version 1.4");
                    returnJson = executeStoredProcedure(null, ds, payloadJson);
//                    break;
                case SEARCH_FILTERED:
                    returnJson = executeFilter(ds, table, payloadJson);
                    break;

                /* User Access */
                case CREATE_MASTER_KEY:
                    returnJson = createMasterKey();
                    break;
                case CREATE_DS_KEY:
                    returnJson = createDsKey(payloadJson);
                    break;
                case LIST_API_KEYS:
                    returnJson = listApiKeys();
                    break;
                case LIST_DS_API_KEYS:
                    returnJson = listDsApiKeys(payloadJson);
                    break;
                case DROP_API_KEY:
                    returnJson = listDsApiKeys(payloadJson);
                    break;

                /* Misc/Debug etc */
                case REPOP:
                    returnJson = executeRepopulateTable(ds, payloadJson);
                    break;
            }

            long executionTime = System.currentTimeMillis() - startTime;
//            logger.debug("Response (" + requestId + "): " + returnJson.toString());
            logger.trace("Executed in (ms): " + executionTime);
            return returnJson.toString();
        } catch (OperationException ex) {
            JSONObject errorJson = new JSONObject(JsonMessages.ERROR_ACKNOWLEDGEMENT);
            try {
                errorJson.put("code", ex.getErrorCode().getErrorCode());
                if (ex.getMessage() != null && !ex.getMessage().isEmpty()) {
                    errorJson.put("cause", ex.getMessage());
                } else {
                    errorJson.put("cause", ex.getErrorCode().getErrorMessage());
                }
//                logger.debug("Response (" + requestId + "): " + errorJson.toString());
                return errorJson.toString();
            } catch (JSONException ex1) {
                //do nothing
            }
        } catch (Exception ex) {
            logger.error(ex.getMessage(), ex);
            JSONObject errorJson = new JSONObject(JsonMessages.ERROR_ACKNOWLEDGEMENT);
            try {
                errorJson.put("code", ErrorCode.UNKNOWN_ERROR.getErrorCode());
                errorJson.put("cause", ErrorCode.UNKNOWN_ERROR.getErrorMessage());
//                logger.debug("Response (" + requestId + "): " + errorJson.toString());
                return errorJson.toString();
            } catch (JSONException ex1) {
                //do nothing
            }
        } finally {

            if (ds != null) {
                requestStore.unregister(ds, requestId);
            }

//            if (command != null) {
//                /* Release acquired locks */
//                switch (command) {
//
//                    /* Cases of database locks with some of additional table locks */
//                    case CREATE_DB:
//                    case CREATE_DS:
//                    case DROP_DB:
//                    case DROP_DS:
//                        masterLockBean.releaseGlobalLock();
//                        break;
//                    case CREATE_TABLE:
//                    case CREATE_COLLECTION:
//                    case DROP_TABLE:
//                    case DROP_COLLECTION:
//                    case RENAME_TABLE:
////                    case LOAD_CODE:
//                        masterLockBean.releaseApplicationLock(app);
//                        break;
//                    /* Cases of table locks */
//                    case TRUNCATE_TABLE:
//                    case ADD_COLUMN:
//                    case DROP_COLUMN:
//                    case RENAME_COLUMN:
//                    case ALTER_COLUMN:
//                    case CHANGE_DATA_TYPE:
//                    case INDEX:
//                    case DROP_INDEX:
//                        masterLockBean.releaseTableLock(app, table);
//                        break;
//                }
//            }
        }

        return null;
    }

    private JSONObject createMasterKey() throws OperationException {
        return new JSONObject(JsonMessages.SUCCESS_ACKNOWLEDGEMENT).put("key", securityManager.createMasterKey());
    }

    private JSONObject createDsKey(final JSONObject payloadJson) throws OperationException {
        if(!payloadJson.has("ds")) {
            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "Missing DS parameter");
        }
        return new JSONObject(JsonMessages.SUCCESS_ACKNOWLEDGEMENT).put("key", securityManager.createDsKey(payloadJson.getString("ds")));
    }

    private JSONObject listApiKeys() throws OperationException {
        JSONArray keysArray = new JSONArray();
        securityManager.getApiKeys().forEach(key -> keysArray.put(key));
        return new JSONObject(JsonMessages.SUCCESS_ACKNOWLEDGEMENT).put("keys", keysArray);
    }

    private JSONObject listDsApiKeys(final JSONObject payloadJson) throws OperationException {
        if(!payloadJson.has("ds")) {
            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "Missing DS parameter");
        }
        JSONArray keysArray = new JSONArray();
        securityManager.getDsApiKeys(payloadJson.getString("ds")).forEach(key -> keysArray.put(key));
        return new JSONObject(JsonMessages.SUCCESS_ACKNOWLEDGEMENT).put("keys", keysArray);
    }

    private JSONObject insert(final String datastore, final String collection, final JSONObject payloadJson) throws OperationException {
        List<Object> dataArray = new ArrayList<>();
        RecordType recordType = null;
        JSONArray jsonArray;
        Query query;

        if(!payloadJson.has("data")) {
            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT,"missing 'data' parameter in payload");
        }
        jsonArray = payloadJson.getJSONArray("data");


        if(payloadJson.has("type")) {
            recordType = RecordType.fromTypeCode(payloadJson.getString("type"));
        } else {
            recordType = RecordType.AUTO;
        }

        /* Convert JSONArray to Collection type */
        for(int i = 0; i < jsonArray.length(); i++) {
            dataArray.add(jsonArray.get(i));
        }

        query = new Query().insertQueryUninferred(datastore, collection, dataArray, recordType);

        /* Add interpreter if present */
        if(payloadJson.has(QueryParams.INTERPRETER.getParam())) {
            query.put(QueryParams.INTERPRETER, payloadJson.getString(QueryParams.INTERPRETER.getParam()));
        }

        /* Add interceptor if present */
        if(payloadJson.has(QueryParams.INTERCEPTOR.getParam())) {
            query.put(QueryParams.INTERCEPTOR, payloadJson.getString(QueryParams.INTERCEPTOR.getParam()));
        }

//        System.out.println("Firing query: " + query.toJsonString());

        return requestHandlingBean.newRequest(query).toJson();
    }

    private JSONObject addColumn(final String datastore, final String collection, final JSONObject jsonObject) throws OperationException {
        String columnName = null;
        final JSONObject dataTypeJson;
        AutoDefineTypes autoDefineType = AutoDefineTypes.NONE;
        IndexTypes indexType = IndexTypes.NONE;

        /* Mandatory attributes */
        try {
            columnName = jsonObject.getString("name");
            dataTypeJson = jsonObject.getJSONObject("type");
        } catch (JSONException ex) {
            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "Could not successfully process payload json");
        }

        /* Read optional auto-define attribute */
        try {
            autoDefineType = AutoDefineTypes.fromString(jsonObject.getString("auto-define"));
        } catch (JSONException ex) {
            //do nothing
        }

        /* Read optional index attribute */
        try {
            indexType = IndexTypes.fromString(jsonObject.getString("index"));
        } catch (JSONException ex) {
            //do nothing
        }

        collectionManager.addColumn(datastore, collection, columnName, FieldTypeFactory.fromJson(dataTypeJson), autoDefineType, indexType);
        try {
            return new JSONObject(JsonMessages.SUCCESS_ACKNOWLEDGEMENT);
        } catch (JSONException ex) {

            //TODO: Notify admin
            logger.error(null, ex);
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "An internal operation error occurred");
        }
    }

    private JSONObject alterColumn(final String datastore, final String collection, final JSONObject jsonObject) throws OperationException {
        String name;
        FieldType dataType = null;
        AutoDefineTypes autoDefineType = null;

        /* Mandatory attributes */
        try {
            name = jsonObject.getString("name");
        } catch (JSONException ex) {
            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "Could not successfully process payload json");
        }

        try {
            dataType = FieldTypeFactory.fromJson(jsonObject.getJSONObject("type"));
        } catch (JSONException ex) {
            //do nothing
        }

        try {
            autoDefineType = AutoDefineTypes.fromString(jsonObject.getString("auto-define"));
        } catch (JSONException ex) {
            //do nothing
        }

        collectionManager.alterColumn(datastore, collection, name, dataType, autoDefineType);
        try {
            return new JSONObject(JsonMessages.SUCCESS_ACKNOWLEDGEMENT);
        } catch (JSONException ex) {

            //TODO: Notify admin
            logger.error(null, ex);
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "An internal operation error occurred");
        }
    }

    private JSONObject bulkImport(final String datastore, final String collection, JSONObject jsonObject) throws OperationException {
        String importType;
        String importFile;
        JSONObject columnMappingJson;
        Map<String, String> columnMappingMap;
        try {
            importType = jsonObject.getString("type");
            columnMappingJson = jsonObject.getJSONObject("column-mapping");
            importFile = jsonObject.getString("file");
        } catch (JSONException ex) {
            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "Query payload could not be parsed correctly");
        }

        columnMappingMap = new HashMap<>();
        Iterator<String> iterator = columnMappingJson.keys();
        while (iterator.hasNext()) {
            try {
                String csvColumnName = iterator.next();
                columnMappingMap.put(csvColumnName, columnMappingJson.getString(csvColumnName));
            } catch (JSONException ex) {
                throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR);
            }
        }

        /* Make JSON request format consistent with operation file format */
        try {
            jsonObject.put("type", OperationTypes.IMPORT.getTypeCode());
            jsonObject.put("import-type", importType);
            jsonObject.put("records", 0);
            jsonObject.put("time-started", -1);
        } catch (JSONException ex) {
            logger.error(null, ex);
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR);
        }

        /* Move import file from public uploads folder to the collection specific import folder */
        File fromFile = new File(importFile);
        File toFile = new File(PathUtil.importFile(datastore, collection, fromFile.getName()));
        try {
            Files.move(fromFile, toFile);
        } catch (IOException ex) {
            logger.error(null, ex);
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR);
        }

        /* Update 'file' param in the json to the new path */
        try {
            jsonObject.put("file", toFile.getAbsolutePath());
        } catch (JSONException ex) {
            logger.error(null, ex);
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR);
        }

        final String opid = operationsManager.registerOperation(datastore, collection, OperationTypes.IMPORT, jsonObject);

        try {
            JSONObject successResponse = new JSONObject(JsonMessages.SUCCESS_ACKNOWLEDGEMENT);
            successResponse.put("opid", opid);
            return successResponse;
        } catch (JSONException ex) {
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR);
        }
    }

    private JSONObject bulkSelect(final String datastore, final String collection, JSONArray jsonArray) throws OperationException {
        JSONObject jsonResponse;
        JSONObject jsonObject;
        JSONArray payloadJsonArray = new JSONArray();

        for (int i = 0; i < jsonArray.length(); i++) {
            try {
                final String key = jsonArray.get(i).toString();
                jsonObject = dataManager.select(datastore, collection, key);
                payloadJsonArray.put(jsonObject);
            } catch (JSONException ex) {
                logger.error(null, ex);
                throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "Primary keys could not be parsed correctly");
            }
        }

        try {
            jsonResponse = new JSONObject(JsonMessages.SUCCESS_ACKNOWLEDGEMENT);
            jsonResponse.put(BQueryParameters.PAYLOAD, payloadJsonArray);
        } catch (JSONException ex) {

            //TODO: Notify admin
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "An internal operation error occurred");
        }

        return jsonResponse;
    }

    private JSONObject changeDataType(final String datastore, final String collection, JSONObject jsonObject) throws OperationException {
        String name;
        FieldType dataType;

        /* Mandatory attributes */
        try {
            name = jsonObject.getString("name");
            dataType = FieldTypeFactory.fromJson(jsonObject.getJSONObject("type"));
        } catch (JSONException ex) {
            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "Could not successfully process payload json");
        }

        collectionManager.changeDataType(datastore, collection, name, dataType);
        try {
            return new JSONObject(JsonMessages.SUCCESS_ACKNOWLEDGEMENT);
        } catch (JSONException ex) {

            //TODO: Notify admin
            logger.error(null, ex);
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "An internal operation error occurred");
        }
    }

    private JSONObject contains(final String datastore, final String collection, String _id) throws OperationException {
        JSONObject responseJson;

        boolean contains = dataManager.exists(datastore, collection, _id);
        try {
            responseJson = new JSONObject(JsonMessages.SUCCESS_ACKNOWLEDGEMENT);
            responseJson.put("contains", contains);
            return responseJson;
        } catch (JSONException ex) {
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "An internal operation error occurred");
        }
    }

    private JSONObject createDatastore(final JSONObject jsonObject) throws OperationException {
        if(!jsonObject.has("name")) {
            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "Missing 'name' parameter in payload");
        }
        final String ds = jsonObject.getString("name");

        Pattern pattern = Pattern.compile("[0-9a-zA-Z$_$-]+");
        Matcher matcher = pattern.matcher(ds);
        if (!matcher.matches()) {
            throw new OperationException(ErrorCode.INVALID_DATASTORE_NAME, ds + " is not a valid ds name");
        }

        Query query = new Query().createDs(ds);
        Query responseQuery = requestHandlingBean.newRequest(query);

        if(responseQuery.isAckSuccess()) {
            return new JSONObject(JsonMessages.SUCCESS_ACKNOWLEDGEMENT);
        } else {
            return new JSONObject(JsonMessages.ERROR_ACKNOWLEDGEMENT);
        }
    }

    private JSONObject dropDatastore(final JSONObject jsonObject) throws OperationException {
        if(!jsonObject.has("name")) {
            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "Missing 'name' parameter in payload");
        }
        final String ds = jsonObject.getString("name");

        Query query = new Query().dropDs(ds);
        Query responseQuery = requestHandlingBean.newRequest(query);

        if(responseQuery.isAckSuccess()) {
            return new JSONObject(JsonMessages.SUCCESS_ACKNOWLEDGEMENT);
        } else {
            return new JSONObject(JsonMessages.ERROR_ACKNOWLEDGEMENT);
        }
    }

    private JSONObject dropDatastore(final String ds) throws OperationException {
        Query query = new Query().dropDs(ds);
        Query responseQuery = requestHandlingBean.newRequest(query);

        if(responseQuery.isAckSuccess()) {
            return new JSONObject(JsonMessages.SUCCESS_ACKNOWLEDGEMENT);
        } else {
            return new JSONObject(JsonMessages.ERROR_ACKNOWLEDGEMENT);
        }
    }

    private JSONObject truncateDs(final JSONObject payloadJson) throws OperationException {
        if(!payloadJson.has("name")) {
            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "Missing 'name' parameter in payload");
        }

        final String datastoreName = payloadJson.getString("name");
        if(datastoreName.isEmpty()) {
            throw new OperationException(ErrorCode.INVALID_QUERY_PARAMETER, "'name' parameter cannot be empty");
        }

        try {
            masterLockBean.acquireApplicationLock(datastoreName);
            final String archiveCode = UUID.randomUUID().toString();
            datastoreManager.truncateDs(datastoreName, archiveCode);
            JSONObject responseJson = new JSONObject(JsonMessages.SUCCESS_ACKNOWLEDGEMENT);
            JSONObject responsePayload = new JSONObject();
            responsePayload.put("arch-code", archiveCode);
            responseJson.put("p", responsePayload);
            return responseJson;
        } catch(InterruptedException ex){
            return new JSONObject(JsonMessages.ERROR_ACKNOWLEDGEMENT);
        } finally {
            masterLockBean.releaseApplicationLock(datastoreName);
        }

    }

    private JSONObject createSearchResponse(final String datastore, final String collection, Set<String> resultSet, boolean load) throws OperationException {
        JSONObject responseJson = new JSONObject();
        JSONArray jsonArray;
        if (load) {
            jsonArray = new JSONArray();
            for (String pk : resultSet) {
                jsonArray.put(dataManager.select(datastore, collection, pk));
            }
        } else {
            jsonArray = new JSONArray(resultSet);
        }

        try {
            responseJson.put("ack", "1");
            responseJson.put("p", jsonArray);
        } catch (JSONException ex) {
            logger.error(null, ex);
        }

        return responseJson;
    }

    @Deprecated
    private JSONObject createTable(final String datastore, final String collection) throws OperationException {
        logger.trace("create-table without schema {}, {}", new Object[]{datastore, collection});
        collectionManager.createTable(datastore, collection);
        try {
            return new JSONObject(JsonMessages.SUCCESS_ACKNOWLEDGEMENT);
        } catch (JSONException ex) {

            //TODO: Notify admin
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "An internal operation error occurred");
        }
    }

    private JSONObject createTable(final String datastore, final JSONObject jsonObject) throws OperationException {
        if(!jsonObject.has("name")) {
            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "Missing 'name' parameter in payload");
        }
        final String name = jsonObject.getString("name");

        if(!jsonObject.has("type")) {
            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "Missing 'type' parameter in payload");
        }
        final TableType tableType = TableType.fromString(jsonObject.getString("type"));

        final ReplicationType replicationType = jsonObject.has("replication-type") ? ReplicationType.fromString(jsonObject.getString("replication-type")) : ReplicationType.DISTRIBUTED;
        if(replicationType == null) {
            throw new OperationException(ErrorCode.INVALID_QUERY_PARAMETER, "Unknown replication-type specified: " + jsonObject.getString("replication-type"));
        }

        final Integer replicationFactor = replicationType == ReplicationType.DISTRIBUTED ? (jsonObject.has("replication-factor") ? jsonObject.getInt("replication-factor") : 0) : null;

        collectionManager.createTable(datastore, name, tableType, replicationType, replicationFactor);
        return new JSONObject(JsonMessages.SUCCESS_ACKNOWLEDGEMENT);
    }

    private JSONObject createUser(final JSONObject jsonObject) throws OperationException {
        String username, password;
        try {
            username = jsonObject.getString("username");
            password = jsonObject.getString("password");
        } catch (JSONException ex) {
            throw new OperationException(ErrorCode.INVALID_QUERY_PARAMETER, "Error in getting data from json string");
        }
        try {
            securityManager.addUser(username, password);
            return new JSONObject(JsonMessages.SUCCESS_ACKNOWLEDGEMENT);
        } catch (BadUsernameException | BadPasswordException ex) {
            throw new OperationException(ErrorCode.USER_CREDENTIALS_INVALID, ex.getMessage());
        }
    }

    /**
     * use cluster delete/remove method
     *
     * @param datastore
     * @param collection
     * @param _id
     * @return
     * @throws OperationException
     */
    @Deprecated
    private JSONObject delete(final String datastore, final String collection, final String _id) throws OperationException {
        dataManager.remove(datastore, collection, _id);
        try {
            return new JSONObject(JsonMessages.SUCCESS_ACKNOWLEDGEMENT);
        } catch (JSONException ex) {
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "An internal operation error occurred");
        }
    }

    private JSONObject dropColumn(final String datastore, final String collection, final JSONObject jsonObject) throws OperationException {
        String columnName = null;

        /* Mandatory attributes */
        try {
            columnName = jsonObject.getString("name");
        } catch (JSONException ex) {
            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "Could not successfully process payload json");
        }

        collectionManager.dropColumn(datastore, collection, columnName);
        try {
            return new JSONObject(JsonMessages.SUCCESS_ACKNOWLEDGEMENT);
        } catch (JSONException ex) {

            //TODO: Notify admin
            logger.error(null, ex);
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "An internal operation error occurred");
        }
    }

    private JSONObject dropIndex(final String datastore, final String collection, final JSONObject jsonObject) throws OperationException {
        String columnName;
        try {
            columnName = jsonObject.getString("name");
        } catch (JSONException ex) {
            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "Payload json of incorrect format");
        }

        indexManager.dropIndex(datastore, collection, columnName);

        try {
            return new JSONObject(JsonMessages.SUCCESS_ACKNOWLEDGEMENT);
        } catch (JSONException ex) {

            //TODO: Notify admin
            logger.error(null, ex);
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "An internal operation error occurred");
        }
    }

    private JSONObject setAutoDefine(final String datastore, final String collection, final JSONObject jsonObject) throws OperationException {
        String columnName;
        AutoDefineTypes autoDefineType;

        try {
            columnName = jsonObject.getString("column");
        } catch (JSONException ex) {
            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "Missing column parameter from payload");
        }

        try {
            autoDefineType = AutoDefineTypes.fromString(jsonObject.getString("auto-define-type"));
        } catch (JSONException ex) {
            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "Missing auto-define-type parameter from payload");
        }

        collectionManager.setAutoDefine(datastore, collection, columnName, autoDefineType);

        return new JSONObject(JsonMessages.SUCCESS_ACKNOWLEDGEMENT);
    }

    private JSONObject dropTable(final String datastore, final String collection) throws OperationException {
        try {
            collectionManager.dropTable(datastore, collection);
            SchemaStore.getInstance().invalidateSchema(datastore, collection);
            return new JSONObject(JsonMessages.SUCCESS_ACKNOWLEDGEMENT);
        } catch (JSONException ex) {

            //TODO: Report to administrators
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "An internal operation error occurred");
        }
    }

    private JSONObject dropUser(final JSONObject jsonObject) throws OperationException {
        String username, password;
        try {
            username = jsonObject.getString("username");
            password = jsonObject.getString("password");
        } catch (JSONException ex) {
            throw new OperationException(ErrorCode.INVALID_QUERY_PARAMETER, "Error in getting data from JSONString");
        }
        try {
            securityManager.deleteUser(username, password);
            return new JSONObject(JsonMessages.SUCCESS_ACKNOWLEDGEMENT);
        } catch (BadUsernameException ex) {
            throw new OperationException(ErrorCode.USER_CREDENTIALS_INVALID, ex.getMessage());
        } catch (InvalidCredentialsException ex) {
            throw new OperationException(ErrorCode.USER_CREDENTIALS_INVALID, "Invalid Credentials supplied for user");
        }
    }

    private JSONObject executeFilter(final String datastore, final String collection, final JSONObject payloadJson) throws OperationException {
        if (!datastoreManager.exists(datastore)) {
            throw new OperationException(ErrorCode.INVALID_QUERY_PARAMETER, "No such dsSet exists");
        }
        if (!collectionManager.exists(datastore, collection)) {
            throw new OperationException(ErrorCode.INVALID_QUERY_PARAMETER, "No such collection exists in given database");
        }
        try {
            String filterName = payloadJson.getString("name");
            String paramsJson = payloadJson.getString("params");
            Gson gson = new Gson();
            Object[] params = gson.fromJson(paramsJson, Object[].class);
            List<String> filteredData = codeExecutor.executeParallelFilter(datastore, collection, filterName, params);
            JSONObject responseJSON = new JSONObject(JsonMessages.SUCCESS_ACKNOWLEDGEMENT);
            responseJSON.put("p", filteredData);
            return responseJSON;
        } catch (JSONException ex) {
            logger.debug("received exception as following: " + ex.getMessage());
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "");
        }
    }

    //TODO: Ask Ashish to remove if not needed
    private JSONObject executeRepopulateTable(final String datastore, final JSONObject requestPayload) throws OperationException, IOException {
        final JSONArray paramJsonArray;
        final String tableName;
        final Object[] params;
        JSONObject responseJson;
        JSONObject responsePayload;

        try {
            tableName = requestPayload.getString("t");
        } catch (JSONException ex) {
            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "Could not successfully retrive table from payload parameters. Check payload format.");
        }
        try {
            //tableName = requestPayload.getString("t");
            paramJsonArray = requestPayload.getJSONArray("params");
        } catch (JSONException ex) {
            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "Could not successfully retrive payload parameters. Check payload format.");
        }

        if (paramJsonArray.length() == 0) {
            params = null;
        } else {

            /* Create an object array of parameters from the json array */
            params = new Object[paramJsonArray.length()];
            for (int i = 0; i < paramJsonArray.length(); i++) {
                try {
                    params[i] = paramJsonArray.get(i);
                } catch (JSONException ex) {
                    throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "Could not successfully retrive payload procedure parameters");
                }
            }
        }

        String dataFileName = (String) params[0];
        boolean header;
        header = "true".equals((String) params[1]);
        String separateChar = (String) params[2];
        String columnsFileName = (String) params[3];


        Object response = null;
        //dataManager.repopulateTable(dsSet, tableName, (String)params[0], header, separateChar, (String)params[2]);

        /* Populate success response */
        responseJson = new JSONObject(JsonMessages.SUCCESS_ACKNOWLEDGEMENT);
        try {
            responsePayload = new JSONObject();
            responsePayload.put("ret", response);
            responseJson.put("p", responsePayload);
            return responseJson;
        } catch (JSONException ex) {
            logger.error(null, ex);
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR);
        }
    }

    private JSONObject executeStoredProcedure(final String requestId, final String datastore, final JSONObject requestPayload) throws OperationException {
        final String procedureName;
        final Object[] params;
        JSONObject responseJson;

        try {
            procedureName = requestPayload.getString("name");
            params = new Gson().fromJson(requestPayload.getString("params"), Object[].class);
        } catch (JSONException ex) {
            logger.debug(null, ex);
            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "Could not successfully retrive payload parameters. Check payload format.");
        }
        /* Execute procedure */
        Object procedureResponse = codeExecutor.executeProcedure(requestId, datastore, procedureName, params);

        responseJson = new JSONObject(JsonMessages.SUCCESS_ACKNOWLEDGEMENT);
        try {
            responseJson.put("p", new Gson().toJson(procedureResponse));
            return responseJson;
        } catch (JSONException ex) {
            logger.error(null, ex);
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR);
        }
    }

    private JSONObject index(final String datastore, final String collection, final JSONObject jsonObject) throws OperationException {
        String columnName;
        IndexTypes indexType;
        OperationLogLevel operationLogLevel = OperationLogLevel.ERROR;
        try {
            columnName = jsonObject.getString("name");
            indexType = IndexTypes.fromString(jsonObject.getString("index"));
            if (jsonObject.has("log-level")) {
                operationLogLevel = OperationLogLevel.fromText(jsonObject.getString("log-level"));
            }
        } catch (JSONException ex) {
            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "Payload json of incorrect format");
        }

        indexManager.index(datastore, collection, columnName, indexType, operationLogLevel);

        try {
            return new JSONObject(JsonMessages.SUCCESS_ACKNOWLEDGEMENT);
        } catch (JSONException ex) {

            //TODO: Notify admin
            logger.error(null, ex);
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "An internal operation error occurred");
        }
    }

    private JSONObject insertViaInterpreter(final String datastore, final String collection, final JSONObject payloadJson) throws OperationException {
        if (!datastoreManager.exists(datastore)) {
            throw new OperationException(ErrorCode.INVALID_QUERY_PARAMETER, "No such datbase exists");
        }
        if (!collectionManager.exists(datastore, collection)) {
            throw new OperationException(ErrorCode.INVALID_QUERY_PARAMETER, "No such collection exists in given database");
        }
        String interpreterName = payloadJson.getString("interpreter");
        JSONArray data = payloadJson.getJSONArray("payload");
        List<JSONObject> tmp = codeExecutor.executeDataInterpreter(datastore, collection, interpreterName, data);
        JSONObject responseJSON = new JSONObject(JsonMessages.SUCCESS_ACKNOWLEDGEMENT);
        responseJSON.put("p", tmp);
        return responseJSON;
    }

    public boolean internalInsert(String datastore, String collection, JSONObject jsonObject) {
        try {
            dataManager.insert(datastore, "db", collection, jsonObject);
            return true;
        } catch (OperationException ex) {
            return false;
        }
    }

    private JSONObject listDatastores() throws OperationException {
        List<String> databases = datastoreManager.listDatabases();
        JSONObject responseJSON = new JSONObject(JsonMessages.SUCCESS_ACKNOWLEDGEMENT);
        JSONObject payloadJson = new JSONObject();
        payloadJson.put("ds", databases);
        responseJSON.put("p", payloadJson);
        return responseJSON;
    }

    private JSONObject dsExists(JSONObject requestPayload) throws OperationException {
        if(!requestPayload.has("ds")) {
            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "Required parameter 'ds' missing");
        }

        final String ds = requestPayload.getString("ds");

        if(ds.isEmpty()) {
            throw new OperationException(ErrorCode.INVALID_QUERY_PARAMETER, "'ds' cannot be empty");
        }

        final boolean exists = datastoreManager.exists(ds);

        JSONObject responseJson = new JSONObject(JsonMessages.SUCCESS_ACKNOWLEDGEMENT);
        JSONObject responsePayload = new JSONObject();
        responsePayload.put("exists", exists);
        responseJson.put("p", responsePayload);
        return responseJson;
    }

    private JSONObject listSchema(final String datastore, final String collection) throws OperationException {
        try {
            JSONObject responseJson = new JSONObject(JsonMessages.SUCCESS_ACKNOWLEDGEMENT);

            if (!collectionManager.exists(datastore, collection)) {
                throw new OperationException(ErrorCode.COLLECTION_INVALID, "No collection found with name: " + collection);
            }

            Schema schema = SchemaStore.getInstance().getSchema(datastore, collection);

            responseJson.put("p", schema.toJSONObject());
            return responseJson;
        } catch (JSONException ex) {

            //TODO: Notify admin
            logger.error(null, ex);
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "An internal operation error occurred");
        }
    }

    /**
     * Maintained for backward support to version 1.3
     */
    @Deprecated
    private JSONObject listTables(final String datastore) throws OperationException {
        try {
            JSONObject responseJson = new JSONObject(JsonMessages.SUCCESS_ACKNOWLEDGEMENT);
            List<String> tableNames = collectionManager.listTables(datastore);
            responseJson.put("tables", tableNames.toArray());
            return responseJson;
        } catch (JSONException ex) {

            //TODO: Notify admin
            logger.error(null, ex);
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "An internal operation error occurred");
        }
    }

    private JSONObject listCollections(final JSONObject requestPayload) throws OperationException {
        try {

            if(!requestPayload.has("ds")) {
                throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "Missing required 'ds' parameter in request payload");
            }

            final String datastore = requestPayload.getString("ds");
            if(datastore.isEmpty()) {
                throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "'ds' parameter in request payload cannot be empty");
            }

            List<String> tableNames = collectionManager.listTables(datastore);

            JSONObject responseJson = new JSONObject(JsonMessages.SUCCESS_ACKNOWLEDGEMENT);
            JSONObject responsePayload = new JSONObject();
            responsePayload.put("c", tableNames.toArray());
            responseJson.put("p", responsePayload);
            return responseJson;
        } catch (JSONException ex) {

            //TODO: Notify admin
            logger.error(null, ex);
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "An internal operation error occurred");
        }
    }

    private JSONObject loadCode(final String datastore, final JSONObject payloadJson) throws OperationException {
        System.out.println("JSON: " + payloadJson.toString());
        final String jarFile = payloadJson.getString("jar");
        codeLoader.loadJar(datastore, jarFile);
        try {
            return new JSONObject(JsonMessages.SUCCESS_ACKNOWLEDGEMENT);
        } catch (JSONException ex) {
            logger.error(null, ex);
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR);
        }
    }

    private JSONObject renameColumn(final String datastore, final String collection, final JSONObject jsonObject) throws OperationException {
        String existingName = null;
        String newName = null;

        /* Mandatory attributes */
        try {
            existingName = jsonObject.getString("name");
            newName = jsonObject.getString("new-name");
        } catch (JSONException ex) {
            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "Could not successfully process payload json");
        }

        collectionManager.renameColumn(datastore, collection, existingName, newName);
        try {
            return new JSONObject(JsonMessages.SUCCESS_ACKNOWLEDGEMENT);
        } catch (JSONException ex) {

            //TODO: Notify admin
            logger.error(null, ex);
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "An internal operation error occurred");
        }
    }

    private JSONObject renameTable(final String datastore, final String collection, final JSONObject jsonObject) throws OperationException {
        try {
            JSONObject responseJson = new JSONObject(JsonMessages.SUCCESS_ACKNOWLEDGEMENT);
            final String newTableName = jsonObject.getString("new-name");
            collectionManager.renameTable(datastore, collection, newTableName);
            SchemaStore.getInstance().invalidateSchema(datastore, collection);
            return responseJson;
        } catch (JSONException ex) {

            //TODO: Notify admin
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "An internal operation error occurred");
        }
    }

    private JSONObject save(final String datastore, final String collection, final JSONObject payloadJson) throws OperationException {
        JSONArray jsonArray;
        RecordType recordType = RecordType.AUTO;
        List<JSONObject> dataArray = new ArrayList<>();

        if(!payloadJson.has("data")) {
            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT,"missing 'data' parameter in payload");
        }
        jsonArray = payloadJson.getJSONArray("data");


        if(payloadJson.has("type")) {
            recordType = RecordType.fromTypeCode(payloadJson.getString("type"));
        }

        if(recordType != RecordType.JSON) {
            throw new OperationException(ErrorCode.OPERATION_NOT_SUPPORTED, "save on data of type " + recordType.getTypeCode() + " not yet supported.");
        }

        /* Convert JSONArray to Collection type */
        for(int i = 0; i < jsonArray.length(); i++) {
            dataArray.add(jsonArray.getJSONObject(i));
        }

        for(JSONObject record : dataArray) {
            dataManager.save(datastore, collection, record);
        }

        try {
            return new JSONObject(JsonMessages.SUCCESS_ACKNOWLEDGEMENT);
        } catch (JSONException ex) {
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "An internal operation error occurred");
        }
    }

    private JSONObject search(final String datastore, final JSONObject payloadJson) throws OperationException {
        Set<String> selectColumns = null;
        final Set<String> selectTables;
        JSONObject jsonObject;
        JSONArray jsonArray;
        final JSONObject responseJson;
        final JSONArray responsePayloadJsonArray;

        try {

            /* Pick up specified tables. At least one must be specified */
            if (!payloadJson.has("from")) {
                throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "from parameter that specifies the tables "
                        + "missing in search query payload");
            }

            jsonArray = payloadJson.getJSONArray("from");
            selectTables = new HashSet<>();
            for (int i = 0; i < jsonArray.length(); i++) {
                selectTables.add(jsonArray.getString(i));
            }

            /* Pick up select clause if present to select only specified columns. Defaulted to 'SELECT *' */
            if (payloadJson.has("select")) {
                jsonArray = payloadJson.getJSONArray("select");
                selectColumns = new HashSet<>();
                for (int i = 0; i < jsonArray.length(); i++) {
                    selectColumns.add(jsonArray.getString(i));
                }
            }

            /* Pick up where clause */
            if (!payloadJson.has("where")) {
                throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "where parameter that specifies the "
                        + "search criteria missing in the search query payload");
            }

            jsonArray = payloadJson.getJSONArray("where");
            if (jsonArray.length() == 0) {
                throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "search query must have at least one where clause specified");
            }
            if (jsonArray.length() > 1) {
                throw new OperationException(ErrorCode.OPERATION_NOT_SUPPORTED, "search query currently supports only one where clause");
            }

            jsonObject = jsonArray.getJSONObject(0);
            if (!jsonObject.has("c")) {
                throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "c parameter missing in where clause: " + jsonObject.toString());
            }
            if (!jsonObject.has("x")) {
                throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "x parameter missing in where clause: " + jsonObject.toString());
            }
            if (!jsonObject.has("v")) {
                throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "v parameter missing in where clause: " + jsonObject.toString());
            }

            Operators operator = Operators.fromCode(jsonObject.getString("x"));

            /* Fire search query for single cause */
            Iterator<JSONObject> iterator = null;
            switch (operator) {
                case EQ:
                case NEQ:
                case GT:
                case GTEQ:
                case LT:
                case LTEQ:
                    System.out.println("The comparator: " + jsonObject.toString());
                    iterator = dataManager.selectWithPattern(datastore, selectTables.toArray()[0].toString(), jsonObject.getString("c"), jsonObject.get("v"), operator);
                    break;
                case IN:
                case NOT_IN:
                    /* Create set from the passed values for IN and NOT-IN operation */
                    Set<Object> referenceValuesSet = new HashSet<>();
                    JSONArray referenceValuesJsonArray = jsonObject.getJSONArray("v");
                    for (int i = 0; i < referenceValuesJsonArray.length(); i++) {
                        referenceValuesSet.add(referenceValuesJsonArray.get(i));
                    }
                    iterator = dataManager.selectWithPattern(datastore, selectTables.toArray()[0].toString(), jsonObject.getString("c"), referenceValuesSet, operator);
                    break;
            }

            /* Create reponse object from the received iterator */
            responseJson = new JSONObject(JsonMessages.SUCCESS_ACKNOWLEDGEMENT);
            responsePayloadJsonArray = new JSONArray();
            while (iterator.hasNext()) {
                responsePayloadJsonArray.put(iterator.next());
            }
            responseJson.put("p", responsePayloadJsonArray);
            return responseJson;
        } catch (JSONException ex) {
            logger.error(null, ex);
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR);
        }
    }

    private JSONObject searchAnd(final String datastore, final String collection, JSONObject jsonObject, final boolean load) throws OperationException {
        JSONObject responseJson = null;
        JSONArray jsonArray;
        Set<Object> searchSet;
        Set<String> resultSet = new HashSet<>();
        Map<String, Set<Object>> searchCriteria = new HashMap<>();
        String searchColumn = null;
        int searchSetSize = Integer.MAX_VALUE;

        /**
         * Find the column to iterator for the search TODO: Change implementation to least index size column
         */
        Iterator<String> iterator = jsonObject.keys();
        while (iterator.hasNext()) {
            String key = iterator.next();
            try {
                jsonArray = jsonObject.getJSONArray(key);

                if (jsonArray.length() < searchSetSize) {
                    searchSetSize = jsonArray.length();
                    searchColumn = key;
                }

                if (!searchCriteria.containsKey(key)) {
                    searchCriteria.put(key, new HashSet<>());
                }

                for (int i = 0; i < jsonArray.length(); i++) {
                    searchCriteria.get(key).add(jsonArray.get(i));
                }
            } catch (JSONException ex) {
                throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR);
            }
        }

        if (searchColumn == null) {
            throw new OperationException(ErrorCode.INVALID_SEARCH_CRITERIA);
        }

        searchSet = searchCriteria.get(searchColumn);
        searchCriteria.remove(searchColumn);

        for (Object columnValue : searchSet) {
            Iterator<String> indexStream = indexManager.readIndexStream(datastore, collection, searchColumn, columnValue);
            if (indexStream == null) {
                continue;
            }

            while (indexStream.hasNext()) {
                boolean allContains = true;
                String pk = indexStream.next();
                for (String column : searchCriteria.keySet()) {
                    boolean columnContains = false;
                    for (Object value : searchCriteria.get(column)) {
                        if (indexManager.contains(datastore, collection, column, value, pk)) {
                            columnContains = true;
                            break;
                        }
                    }

                    if (!columnContains) {
                        allContains = false;
                        break;
                    }
                }

                if (allContains) {
                    resultSet.add(pk);
                    if (resultSet.size() >= limit) {
                        return createSearchResponse(datastore, collection, resultSet, load);
                    }
                }
            }
        }

        return createSearchResponse(datastore, collection, resultSet, load);
    }

    private JSONObject searchOr(final String datastore, final String collection, JSONObject jsonObject, final boolean load) throws OperationException {
        JSONObject responseJson = new JSONObject();
        JSONArray jsonArray = null;
        String columnName;
        List<String> dataList = new ArrayList<>();

        try {
            Iterator<String> iterator = jsonObject.keys();
            while (iterator.hasNext()) {
                columnName = iterator.next();
                jsonArray = (JSONArray) jsonObject.getJSONArray(columnName);
                for (int i = 0; i < jsonArray.length(); i++) {
                    Iterator<String> recordIterator = indexManager.readIndexStream(datastore, collection, columnName, jsonArray.getString(i));
                    while (recordIterator.hasNext()) {
                        dataList.add(recordIterator.next());

                        if (dataList.size() >= limit) {
                            break;
                        }
                    }
                }
            }
        } catch (JSONException ex) {
            try {
                responseJson.put("ack", "0");
            } catch (JSONException ex1) {
                logger.error(null, ex1);
            }
            return responseJson;
        }

        if (load) {
            jsonArray = new JSONArray();
            for (String pk : dataList) {
                jsonArray.put(dataManager.select(datastore, collection, pk));
            }
        } else {
            jsonArray = new JSONArray(dataList);
        }

        try {
            responseJson.put("ack", "1");
            responseJson.put("p", jsonArray);
        } catch (JSONException ex) {
            logger.error(null, ex);
        }

        return responseJson;
    }

    /**
     * use cluster select method
     *
     * @param datastore
     * @param collection
     * @param _id
     * @return
     * @throws OperationException
     * @throws InterruptedException
     */
    @Deprecated
    private JSONObject select(final String datastore, final String collection, final String _id) throws OperationException {
        JSONObject jsonResponse = null;
        try {
//            transactionLocking.acquireLock(app, collection, _id, LockType.READ);
            JSONObject responsePayloadJson = dataManager.select(datastore, collection, _id);

            try {
                jsonResponse = new JSONObject(JsonMessages.SUCCESS_ACKNOWLEDGEMENT);
                jsonResponse.put(BQueryParameters.PAYLOAD, responsePayloadJson);
            } catch (JSONException ex) {

                //TODO: Notify admin
                throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "An internal operation error occurred");
            }
        } finally {
//            transactionLocking.releaseLock(app, collection, _id, LockType.READ);
        }

        return jsonResponse;
    }

    private JSONObject selectAll(final String datastore, final String collection) {
        JSONObject responseJson;
        JSONArray keysArray = new JSONArray();

        List<String> keysList;
        try {
            keysList = fileManager.selectAll(datastore, collection);
        } catch (OperationException ex) {
            logger.error("SelectAll failed for dsSet->{}, collection->{}", datastore, collection);
            return JSONOperationException.create(ex);
        }

        try {
            responseJson = new JSONObject(JsonMessages.SUCCESS_ACKNOWLEDGEMENT);

            for (String key : keysList) {
                keysArray.put(key);
            }
            responseJson.put("keys", keysArray);

            return responseJson;
        } catch (JSONException ex) {
            try {
                return new JSONObject(JsonMessages.ERROR_ACKNOWLEDGEMENT);
            } catch (JSONException ex1) {
                //do nothing
            }
        }

        return null;
    }

    private JSONObject tableExists(final String datastore, final String collection) throws OperationException {
        try {
            JSONObject responseJson = new JSONObject(JsonMessages.SUCCESS_ACKNOWLEDGEMENT);
            boolean exists = collectionManager.exists(datastore, collection);
            responseJson.put("exists", exists);
            return responseJson;
        } catch (JSONException ex) {

            //TODO: Notify admin
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "An internal operation error occurred");
        }
    }

    private JSONObject collectionExists(final JSONObject requestPayload) throws OperationException {
        if(!requestPayload.has("ds")) {
            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "Required parameter 'ds' missing");
        }

        if(!requestPayload.has("c")) {
            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "Required parameter 'c' missing");
        }

        final String ds = requestPayload.getString("ds");
        final String collection = requestPayload.getString("c");

        if(ds.isEmpty()) {
            throw new OperationException(ErrorCode.INVALID_QUERY_PARAMETER, "'ds' cannot be empty");
        }

        if(collection.isEmpty()) {
            throw new OperationException(ErrorCode.INVALID_QUERY_PARAMETER, "'c' cannot be empty");
        }

        boolean exists = collectionManager.exists(ds, collection);

        JSONObject responseJson = new JSONObject(JsonMessages.SUCCESS_ACKNOWLEDGEMENT);
        JSONObject responsePayload = new JSONObject();
        responsePayload.put("exists", exists);
        responseJson.put("p", responsePayload);
        return responseJson;
    }

    private JSONObject truncateTable(final String datastore, final String collection) throws OperationException {
        collectionManager.truncateTable(datastore, collection);

        try {
            return new JSONObject(JsonMessages.SUCCESS_ACKNOWLEDGEMENT);
        } catch (JSONException ex) {

            //TODO: Notify admin
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "An internal operation error occurred");
        }
    }

    private JSONObject startDsFtp(final String datastore) throws OperationException {
        final String password = ftpServiceManager.startFtpService(datastore);
        final JSONObject responseJson = new JSONObject(JsonMessages.SUCCESS_ACKNOWLEDGEMENT);
        JSONObject responsePayload = new JSONObject();
        responsePayload.put("pwd", password);
        responseJson.put("p", responsePayload);
        return responseJson;
    }

    private JSONObject stopDsFtp(final String datastore) throws OperationException {
        ftpServiceManager.stopFtpService(datastore);
        return JsonMessages.SUCCESS_ACKNOWLEDGEMENT_OBJECT;
    }

    private JSONObject tableauRequiresSync() throws OperationException {
        try {
            JSONObject responseJson = new JSONObject(JsonMessages.SUCCESS_ACKNOWLEDGEMENT);
            responseJson.put("p", tableauPublishStore.requiresTableauSync());
            return responseJson;
        } catch (JSONException ex) {

            //TODO: Notify admin
            logger.error(null, ex);
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "An internal operation error occurred");
        }
    }
}