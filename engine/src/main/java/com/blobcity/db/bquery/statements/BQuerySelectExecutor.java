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

package com.blobcity.db.bquery.statements;

import com.blobcity.db.bsql.BSqlDataManager;
import com.blobcity.db.master.MasterExecutable;
import com.blobcity.db.master.MasterStore;
import com.blobcity.db.master.executors.data.SelectMaster;
import com.blobcity.db.exceptions.ErrorCode;
import com.blobcity.db.exceptions.OperationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import com.blobcity.util.json.JsonMessages;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 *
 * @author sanketsarang
 */
@Component
@Deprecated
public class BQuerySelectExecutor {

    @Autowired
    private MasterStore masterStore;

    @Autowired
    private BSqlDataManager dataManager;

    public JSONObject execute(JSONObject queryJson) throws OperationException {
        BQuerySelectStatement bQuerySelectStatement = BQuerySelectStatement.fromJson(queryJson);
        return execute(bQuerySelectStatement);
    }
    
    public JSONObject execute(BQuerySelectStatement bQuerySelectStatement) throws OperationException {

        final String ds = bQuerySelectStatement.getApp();
        final String collection = bQuerySelectStatement.getTable();
        final String pk = bQuerySelectStatement.getPk();

        JSONObject record = dataManager.select(ds, collection, pk);

        JSONObject responseJson = new JSONObject();
        responseJson.put("ack","1");
        responseJson.put("time", "10");
        responseJson.put("p", record);

        return responseJson;

//        return JsonMessages.errorWithCause(ErrorCode.DEPRICATED.getErrorCode());

//        MasterExecutable masterExecutable = new SelectMaster(bQuerySelectStatement.getApp(), bQuerySelectStatement.toJson().toString());
//        masterStore.register(masterExecutable.getRequestId(), masterExecutable);
//        Future<Object> future = masterExecutable.execute();
//        try {
//            return (JSONObject) future.get();
//        } catch (InterruptedException | ExecutionException ex) {
//            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, ex);
//        } finally {
//            masterStore.unregister(masterExecutable.getRequestId());
//        }
    }
}