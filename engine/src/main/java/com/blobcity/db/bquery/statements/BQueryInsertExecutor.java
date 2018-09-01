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

import com.blobcity.db.master.executors.data.InsertMaster;
import com.blobcity.db.master.MasterExecutable;
import com.blobcity.db.master.MasterStore;
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
public class BQueryInsertExecutor {
    
    @Autowired
    private MasterStore masterStore;

    public JSONObject execute(JSONObject queryJson) throws OperationException {
        BQueryInsertStatement bQueryInsertStatement = BQueryInsertStatement.fromJson(queryJson);
        return execute(bQueryInsertStatement);
    }

    public JSONObject execute(BQueryInsertStatement bQueryInsertStatement) throws OperationException {

        return JsonMessages.errorWithCause(ErrorCode.DEPRICATED.getErrorCode());
        
//        /** REMOVED ON 20 MARCH 2016 AS AN AUTODEFINED PRIMARY KEY NAMED _id WAS FORCED ON ALL TABLES
//         *
//        if (bQueryInsertStatement.getPk() == null) {
//            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "Primary key value has to be set in "
//                    + "internal execution layer for insert to succeed");
//        }
//        *
//        */
//
//        MasterExecutable masterExecutable = new InsertMaster(bQueryInsertStatement);
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
