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
import com.blobcity.db.cluster.nodes.ProximityNodesStore;
import com.blobcity.db.code.procedures.ProcedureStoreBean;
import com.blobcity.db.config.ConfigBean;
import com.blobcity.db.config.ConfigProperties;
import com.blobcity.db.constants.BQueryCommands;
import com.blobcity.db.constants.BQueryParameters;
import com.blobcity.db.exceptions.ErrorCode;
import com.blobcity.db.exceptions.OperationException;
import com.blobcity.db.export.ExportType;
import com.blobcity.db.operations.OperationStatus;
import com.blobcity.db.operations.OperationTypes;
import com.blobcity.db.operations.OperationsManager;
import com.blobcity.db.security.exceptions.BadPasswordException;
import com.blobcity.db.security.exceptions.BadUsernameException;
import com.blobcity.db.security.exceptions.InvalidCredentialsException;
import com.blobcity.db.sql.util.PathUtil;
import com.blobcity.db.util.JSONOperationException;
import com.blobcity.lib.database.bean.manager.interfaces.engine.BQueryExecutor;
import com.blobcity.util.json.JsonMessages;
import com.google.common.io.Files;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 *
 * @author sanketsarang
 */
@Component
public class BQueryAdminBean implements BQueryExecutor {
    
    @Autowired
    private OperationsManager operationsManager;
    @Autowired
    private BSqlCollectionManager tableManager;
    @Autowired
    private ProximityNodesStore nodeStore;
    @Autowired
    private ConfigBean configBean;
    @Autowired
    private ProcedureStoreBean procedureStore;
    @Autowired
    private com.blobcity.db.security.SecurityManagerBean securityManager;
    private static final Logger logger = LoggerFactory.getLogger(BQueryAdminBean.class.getName());
    
    @Override
    public String runQuery(final String jsonString) {
        String app = null;
        String table = null;
        BQueryCommands command = null;
        JSONObject jsonObject;
        JSONObject payloadJson = null;
        JSONObject returnJson = null;
        
        try {
            jsonObject = new JSONObject(jsonString);
        } catch (JSONException ex) {
            logger.debug("Failed to parse input JSON", ex);
            return JsonMessages.ERROR_ACKNOWLEDGEMENT;
        }
        try {
            try {
                try {
                    app = jsonObject.getString(BQueryParameters.ACCOUNT);
                } catch (JSONException ex) {
                    app = null;
                }
                
                try {
                    table = jsonObject.getString(BQueryParameters.TABLE);
                } catch (JSONException ex) {
                    table = null;
                }
                
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
            
            logger.trace("parsed command value: {}", command);
            
            switch (command) {
                case BULK_IMPORT:
                    returnJson = bulkImport(app, table, payloadJson);
                    break;
                case LIST_OPS:
                    returnJson = listOps(app, table);
                    break;
                case USAGE:
                    returnJson = usage();
                    break;
                case SET_LIMITS:
                    returnJson = setLimits(payloadJson);
                    break;
                case RESET_USAGE:
                    returnJson = resetUsage(jsonObject.getJSONArray("items"));
                    break;
                case LIST_TRIGGERS:
                    returnJson = listTriggers();
                    break;
                case CHANGE_TRIGGER:
                    returnJson = changeTrigger(payloadJson);
                    break;
                case LIST_FILTERS:
                    returnJson = listFilters();
                    break;
                case LIST_NODES:
                    returnJson = listNodes();
                    break;
                case REGISTER_TRIGGER:
                    returnJson = registerTrigger(payloadJson);
                    break;
                case UNREGISTER_TRIGGER:
                    returnJson = unregisterTrigger(payloadJson);
                    break;
                case BULK_EXPORT:
                    returnJson = bulkExport(app, table, payloadJson);
                    break;
                case CHANGE_PASSWORD:
                    returnJson = changePassword(payloadJson);
                    break;
                case ADD_USER:
                    returnJson = addUser(payloadJson);
                    break;
                case DROP_USER:
                    returnJson = deleteUser(payloadJson);
                    break;
                case VERIFY_CREDENTIALS:
                    returnJson = verifyCredentials(payloadJson);
                case NODE_ID:
                    returnJson = getNodeId(payloadJson);
                case ADD_NODE:
                    returnJson = addNode(payloadJson);
                    break;
            }
            logger.trace("returnJSON value computed: {}", returnJson == null ? "null" : returnJson.toString());
            return returnJson.toString();
        } catch (OperationException ex) {
            logger.debug("OperationException caught", ex);
            JSONObject error = JSONOperationException.create(ex);
            if (error != null) {
                return error.toString();
            }
        } catch (Exception ex) {
            //do nothing
            logger.debug("Exception swallowed", ex);
        } finally {

            /* Release acquired locks */
            switch (command) {
                
            }
        }
        logger.debug("Reached end of function. Returning ack:0");
        return JsonMessages.ERROR_ACKNOWLEDGEMENT;
    }
    
    private JSONObject verifyCredentials(final JSONObject jsonObject) {
        final String username = jsonObject.optString("username");
        final String password = jsonObject.optString("password");
        if (StringUtils.isBlank(username) || StringUtils.isBlank(password)) {
            return JsonMessages.errorWithCause("username and password are required");
        }
        if (securityManager.verifyCredentials(username, password)) {
            return JsonMessages.SUCCESS_ACKNOWLEDGEMENT_OBJECT;
        } else {
            return JsonMessages.errorWithCause("Invalid Credentials");
        }
    }
    
    private JSONObject changePassword(final JSONObject jsonObject) throws OperationException {
        if (jsonObject == null) {
            logger.trace("changePassword(): null arg");
            return JsonMessages.errorWithCause(ErrorCode.INVALID_QUERY_FORMAT.getErrorMessage());
        }
        final String username = jsonObject.optString("username");
        final String oldPassword = jsonObject.optString("password");
        final String newPassword = jsonObject.optString("newPassword");
        logger.trace("changePassword(): {}", username);
        try {
            securityManager.changePassword(username, oldPassword, newPassword);
        } catch (InvalidCredentialsException ex) {
            logger.debug("Invalid Credentials for user: " + username, ex);
            return JsonMessages.errorWithCause(ex.getMessage());
        } catch (BadPasswordException ex) {
            logger.debug("Bad password", ex);
            return JsonMessages.errorWithCause(ex.getMessage());
        }
        return JsonMessages.SUCCESS_ACKNOWLEDGEMENT_OBJECT;
    }
    
    private JSONObject addUser(final JSONObject jsonObject) throws OperationException {
        final String username = jsonObject.optString("username");
        final String password = jsonObject.optString("password");
        logger.trace("addUser(): {}", username);
        try {
            securityManager.addUser(username, password);
        } catch (BadUsernameException ex) {
            logger.debug("Bad username", ex);
            return JsonMessages.errorWithCause(ex.getMessage());
        } catch (BadPasswordException ex) {
            logger.debug("Bad password", ex);
            return JsonMessages.errorWithCause(ex.getMessage());
        }
        return JsonMessages.SUCCESS_ACKNOWLEDGEMENT_OBJECT;
    }
    
    private JSONObject deleteUser(final JSONObject jsonObject) throws OperationException {
        final String username = jsonObject.optString("username");
        final String password = jsonObject.optString("password");
        logger.trace("deleteUser(): {}", username);
        try {
            securityManager.deleteUser(username, password);
        } catch (BadUsernameException ex) {
            logger.debug("Bad username: " + username, ex);
            return JsonMessages.errorWithCause(ex.getMessage());
        } catch (InvalidCredentialsException ex) {
            logger.debug("Invalid Credentials", ex);
            return JsonMessages.errorWithCause(ex.getMessage());
        }
        return JsonMessages.SUCCESS_ACKNOWLEDGEMENT_OBJECT;
    }
    
    private JSONObject bulkImport(final String app, final String table, JSONObject jsonObject) throws OperationException {
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

        /* Move import file from public uploads folder to the table specific import folder */
        File fromFile = new File(importFile);
        File toFile = new File(PathUtil.importFile(app, table, fromFile.getName()));
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
        
        final String opid = operationsManager.registerOperation(app, table, OperationTypes.IMPORT, jsonObject);
        
        try {
            JSONObject successResponse = new JSONObject(JsonMessages.SUCCESS_ACKNOWLEDGEMENT);
            successResponse.put("opid", opid);
            return successResponse;
        } catch (JSONException ex) {
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR);
        }
    }
    
    private JSONObject listOps(final String app, final String table) throws OperationException {
        JSONObject responseJson;
        JSONObject opsJson;
        JSONObject opJson;
        JSONObject jsonObject;
        
        if (!tableManager.exists(app, table)) {
            throw new OperationException(ErrorCode.COLLECTION_INVALID, "A table with the name: " + table + " does not exist inside application: " + app);
        }
        
        List<String> opids = operationsManager.getOperations(app, table);
        opsJson = new JSONObject();
        for (String opid : opids) {
            try {
                String operationContents = operationsManager.readOperationFile(app, table, opid);
                
                if (operationContents.isEmpty()) {
                    opsJson.put(opid, new JSONObject());
                    continue;
                }
                
                jsonObject = new JSONObject(operationContents);
                opJson = new JSONObject();

                /* Add common fields*/
                opJson.put("type", jsonObject.get("type"));
                if (jsonObject.has("records")) {
                    opJson.put("records", jsonObject.get("records"));
                }
                if (jsonObject.has("time-started")) {
                    opJson.put("time-started", jsonObject.get("time-started"));
                } else {
                    opJson.put("time-started", -1); //default
                }
                if (jsonObject.has("time-stopped")) {
                    opJson.put("time-stopped", jsonObject.get("time-started"));
                } else {
                    opJson.put("time-started", -1); //default
                }
                if (jsonObject.has("status")) {
                    opJson.put("status", jsonObject.get("status"));
                } else {
                    opJson.put("status", OperationStatus.NOT_STARTED.getStatusCode());//default
                }
                if (jsonObject.has("log")) {
                    opJson.put("log", jsonObject.get("log"));
                }

                /* Add type specific fields */
                switch (jsonObject.getString("type")) {
                    case "IMP":
                        if (jsonObject.has("import-type")) {
                            opJson.put("import-type", jsonObject.get("import-type"));
                        }
                        break;
                    case "IND":
                        if (jsonObject.has("column")) {
                            opJson.put("column", jsonObject.get("column"));
                        }
                        if (jsonObject.has("index-type")) {
                            opJson.put("index-type", jsonObject.get("index-type"));
                        }
                        break;
                    case "EXP":
                        if (jsonObject.has("export-type")) {
                            opJson.put("export-type", jsonObject.get("export-type"));
                        }
                        if (jsonObject.has("file")) {
                            opJson.put("file", PathUtil.exportFile(app, jsonObject.getString("file")));
                        }
                        break;
                    default:
                        opJson = jsonObject;
                }
                opsJson.put(opid, opJson);
            } catch (JSONException ex) {
                logger.error("Could not successfully process JSON for operation: {} in app: {} for table: {}", new Object[]{opid, app, table});
                throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR);
            }
        }
        
        try {
            responseJson = new JSONObject(JsonMessages.SUCCESS_ACKNOWLEDGEMENT);
            responseJson.put("ops", opsJson);
            return responseJson;
        } catch (JSONException ex) {
            logger.error(null, ex);
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR);
        }
    }
    
    private JSONObject usage() throws OperationException {
        throw new OperationException(ErrorCode.OPERATION_NOT_SUPPORTED);
    }
    
    private JSONObject setLimits(JSONObject jsonObject) throws OperationException {
        throw new OperationException(ErrorCode.OPERATION_NOT_SUPPORTED);
    }
    
    private JSONObject resetUsage(JSONArray jsonArray) throws OperationException {
        throw new OperationException(ErrorCode.OPERATION_NOT_SUPPORTED);
    }
    
    private JSONObject listTriggers() throws OperationException {
        throw new OperationException(ErrorCode.OPERATION_NOT_SUPPORTED);
    }
    
    private JSONObject changeTrigger(JSONObject payloadJson) throws OperationException {
        throw new OperationException(ErrorCode.OPERATION_NOT_SUPPORTED);
    }
    
    private JSONObject listFilters() throws OperationException {
        throw new OperationException(ErrorCode.OPERATION_NOT_SUPPORTED);
    }
    
    private JSONObject listNodes() throws OperationException {
        try {
            JSONObject responseJson = new JSONObject(JsonMessages.SUCCESS_ACKNOWLEDGEMENT);
            responseJson.put("nodes", nodeStore.listNodes());
            return responseJson;
        } catch (JSONException ex) {
            logger.error(null, ex);
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR);
        }
    }
    
    private JSONObject registerTrigger(JSONObject payloadJson) throws OperationException {
        throw new OperationException(ErrorCode.OPERATION_NOT_SUPPORTED);
    }
    
    private JSONObject registerProcedure(final String app, final JSONObject payloadJson) throws OperationException {
        final String fullyQualifiedClassName;
        try {
            fullyQualifiedClassName = payloadJson.getString("class");
        } catch (JSONException ex) {
            logger.error(null, ex);
            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "Could not find/parse required payload parameter 'class'");
        }
        
        procedureStore.loadClass(app, fullyQualifiedClassName);
        
        try {
            return new JSONObject(JsonMessages.SUCCESS_ACKNOWLEDGEMENT);
        } catch (JSONException ex) {
            logger.error(null, ex);
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR);
        }
    }
    
    private JSONObject unregisterTrigger(JSONObject jsonObject) throws OperationException {
        throw new OperationException(ErrorCode.OPERATION_NOT_SUPPORTED);
    }
    
    private JSONObject unregisterProcedure(JSONObject jsonObject) throws OperationException {
        throw new OperationException(ErrorCode.OPERATION_NOT_SUPPORTED);
    }

    private JSONObject bulkExport(final String app, final String table, final JSONObject payloadJson) throws OperationException {
        JSONObject jsonObject = new JSONObject();
        final String exportType;
        final String exportFile;
        
        if (!payloadJson.has("file")) {
            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "Required parameter 'file' missing");
        }
        
        if (!payloadJson.has("type")) {
            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "Required parameter 'export-type' missing");
        }
        
        try {
            exportType = payloadJson.getString("type");
            exportFile = payloadJson.getString("file");
        } catch (JSONException ex) {
            logger.warn("export-file parameter missing in request JSON", ex);
            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "Missing required payload paramters");
        }
        
        if (ExportType.valueOf(exportType) == null) {
            throw new OperationException(ErrorCode.INVALID_QUERY_FORMAT, "Invalid 'type' parameter in payload. Value: " + exportType + " does not match a valid export format");
        }


        /* Make JSON request format consistent with operation file format */
        try {
            jsonObject.put("file", exportFile);
            jsonObject.put("type", OperationTypes.EXPORT.getTypeCode());
            jsonObject.put("export-type", exportType);
            jsonObject.put("records", 0);
            jsonObject.put("time-started", -1);
        } catch (JSONException ex) {
            logger.error(null, ex);
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR);
        }
        
        final String opid = operationsManager.registerOperation(app, table, OperationTypes.EXPORT, jsonObject);
        
        try {
            JSONObject successResponse = new JSONObject(JsonMessages.SUCCESS_ACKNOWLEDGEMENT);
            successResponse.put("opid", opid);
            return successResponse;
        } catch (JSONException ex) {
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR);
        }
    }
    
    private JSONObject addNode(final JSONObject payloadJson) throws OperationException {
        final String nodeId = payloadJson.getString("node-id");
        
        throw new OperationException(ErrorCode.OPERATION_NOT_SUPPORTED);
    }
    
    private JSONObject getNodeId(final JSONObject payloadJson) throws OperationException {
        JSONObject returnJson = new JSONObject();

        /* return self node-id if payload is null */
        if (payloadJson == null) {
            returnJson.put("node-id", configBean.getProperty(ConfigProperties.NODE_ID));
            return returnJson;
        }
        
        final String ipAddress = payloadJson.getString("ip");
        
        
        return null;
    }
}
