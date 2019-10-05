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

package com.blobcity.db.bquery.internal.statements;

import com.blobcity.db.bsql.BSqlDataManager;
import com.blobcity.db.bsql.BSqlCollectionManager;
import com.blobcity.db.constants.BQueryCommands;
import com.blobcity.db.constants.BQueryParameters;
import com.blobcity.db.exceptions.ErrorCode;
import com.blobcity.db.exceptions.OperationException;
import com.blobcity.db.schema.Schema;
import com.blobcity.db.schema.beans.SchemaStore;
import com.blobcity.util.json.JsonMessages;
import java.util.Set;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 *
 * @author sanketsarang
 */
@Component
@Deprecated
public class InternalBQueryExecutor {

    @Autowired
    private BSqlDataManager dataManager;
    @Autowired
    private BSqlCollectionManager collectionManager;

    public JSONObject executeQuery(JSONObject queryJson) throws OperationException {
        JSONObject returnJson = null;
        // this is the name of dsSet
        final String app = queryJson.getString(BQueryParameters.ACCOUNT);
        final BQueryCommands command = BQueryCommands.fromString(queryJson.getString(BQueryParameters.QUERY));
        final String collection;

        if (queryJson.has("t")) {
            collection = queryJson.getString("t");
        } else {
            collection = null;
        }

        if (command == null) {
            throw new OperationException(ErrorCode.INVALID_QUERY, "The query: "
                    + queryJson.getString(BQueryParameters.QUERY) + " could not be recognized as a valid query");
        }

        switch (command) {
            case SELECT:
                returnJson = select(app, collection, queryJson.getString(BQueryParameters.PRIMARY_KEY));
                break;
            case INSERT:
                returnJson = insert(app, collection, queryJson.getJSONObject(BQueryParameters.PAYLOAD));
                break;
            case UPDATE:
                returnJson = update(app, collection, queryJson.getJSONObject(BQueryParameters.PAYLOAD));
                break;
            case DELETE:
                returnJson = delete(app, collection, queryJson.getString(BQueryParameters.PRIMARY_KEY));
        }

        return returnJson;
    }

    private JSONObject select(final String datastore, final String collection, final String _id) throws OperationException {
        JSONObject jsonResponse = null;
        try {
//            transactionLocking.acquireLock(dsSet, collection, _id, LockType.READ);
            JSONObject responsePayloadJson = dataManager.select(datastore, collection, _id);

            try {
                jsonResponse = new JSONObject(JsonMessages.SUCCESS_ACKNOWLEDGEMENT);
                jsonResponse.put(BQueryParameters.PAYLOAD, responsePayloadJson);
            } catch (JSONException ex) {

                //TODO: Notify admin
                throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "An internal operation error occurred");
            }
        } finally {
//            transactionLocking.releaseLock(dsSet, collection, _id, LockType.READ);
        }

        return jsonResponse;
    }

    private JSONObject insert(final String datastore, final String collection, final JSONObject insertJSON) throws OperationException {
        JSONObject responseJson;
        JSONObject responsePayloadJson;

        Schema schema = SchemaStore.getInstance().getSchema(datastore, collection);
        
        /* If schema is flexible, add any missing columns before performing the insert operation */
        if(schema.isFlexibleSchema()) {
            Set<String> missingColumnNames = schema.getMissingColumnNames(insertJSON.keySet());
            for (String missingColumnName : missingColumnNames) {
                
                //TODO: We need a smarter way of inferring field types at this point
                collectionManager.addInferedColumn(datastore, collection, missingColumnName);
//                collectionManager.addColumn(dsSet, collection, missingColumnName, new StringField(Types.STRING), AutoDefineTypes.NONE, IndexTypes.NONE);
            }
        }
        
        responsePayloadJson = dataManager.insert(datastore, collection, insertJSON);

        //Complete payload is returned so that values of auto-defined primary keys can be communicated with the parent application
        try {
            responseJson = new JSONObject(JsonMessages.SUCCESS_ACKNOWLEDGEMENT);
            responseJson.put("p", responsePayloadJson);
            return responseJson;
        } catch (JSONException ex) {

            //TODO: Notify admin
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "An internal operation error occurred");
        }
    }

    private JSONObject update(final String datastore, final String collection, final JSONObject updateJSON) throws OperationException {
        dataManager.save(datastore, collection, updateJSON);
        try {
            return new JSONObject(JsonMessages.SUCCESS_ACKNOWLEDGEMENT);
        } catch (JSONException ex) {
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "An internal operation error occurred");
        }
    }
    
    private JSONObject delete(final String datastore, final String collection, final String _id) throws OperationException {
        dataManager.remove(datastore, collection, _id);
        try {
            return new JSONObject(JsonMessages.SUCCESS_ACKNOWLEDGEMENT);
        } catch (JSONException ex) {
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "An internal operation error occurred");
        }
    }
}
