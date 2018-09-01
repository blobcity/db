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

package com.blobcity.db.master.executors.data;

import com.blobcity.db.bquery.statements.BQueryUpdateStatement;
import com.blobcity.db.cluster.messaging.messages.Message;
import com.blobcity.db.exceptions.ErrorCode;
import com.blobcity.db.exceptions.OperationException;
import com.blobcity.db.master.AbstractCommitMaster;
import com.blobcity.db.master.MasterExecutable;
import com.blobcity.lib.query.Query;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import org.json.JSONObject;

/**
 *
 * @author sanketsarang
 */
@Deprecated
public class UpdateMaster extends AbstractCommitMaster implements MasterExecutable {

    private final String app;
    private Semaphore semaphore;
    private final BQueryUpdateStatement bQueryUpdateStatement;
    private final String sql;
    private JSONObject responseJson = null;
    private boolean queryComplete = false;
    private Map<String, Message> responseMessageMap = new HashMap<>();
    

    public UpdateMaster(final BQueryUpdateStatement bQueryUpdateStatement) {
        super(null);
        this.bQueryUpdateStatement = bQueryUpdateStatement;
        this.sql = null;
        this.app = bQueryUpdateStatement.getApp();
    }

    public UpdateMaster(final String app, final String sql) {
        super(null);
        this.app = app;
        this.sql = sql;
        this.bQueryUpdateStatement = null;
    }

    public Future<Object> execute() throws OperationException {
        return null;
//        if (bQueryUpdateStatement == null) {
//            return new AsyncResult<>(executeSQL());
//        } else {
//            return new AsyncResult<>(executeJSON());
//        }
    }

    public void notifyMessage(String nodeId, Message message) {
//        if (queryComplete) {
//            return;
//        }
//
//        if(responseMessageMap.containsKey(nodeId)) {
//            return;
//        }
//
//        responseMessageMap.put(nodeId, message);
//
//        final JsonResultSetMessage jsonResultSetMessage = (JsonResultSetMessage) message;
//        if(responseJson == null
//                && (JsonUtil.isAck(jsonResultSetMessage.getJsonResult())
//                || responseMessageMap.keySet().size() == clusterNodesStore.getAllNodes().size())) {
//            responseJson = jsonResultSetMessage.getJsonResult();
//        }
//
//        if(responseMessageMap.keySet().size() == clusterNodesStore.getAllNodes().size()) {
//            queryComplete = true;
//            semaphore.release();
//        }
    }

    public BQueryUpdateStatement getbQueryInsertStatement() {
        return bQueryUpdateStatement;
    }

    private Object executeJSON() throws OperationException {
        return null;
//        semaphore = new Semaphore(1);
//        semaphore.acquireUninterruptibly();
//
//        JsonQueryMessage jsonQueryMessage = new JsonQueryMessage();
//        jsonQueryMessage.setRequestId(getRequestId());
//        jsonQueryMessage.setMasterNodeId(clusterNodesStore.getSelfId());
//        jsonQueryMessage.setTargetNodeId(null); //indicates broadcast to all nodes
//        jsonQueryMessage.setQueryJson(bQueryUpdateStatement.toJson());
//
//        clusterMessaging.sendMessage(jsonQueryMessage);
//
//        /* Wait till response is acquired */
//        semaphore.acquireUninterruptibly();
//
//        return responseJson;
    }

    private Object executeSQL() throws OperationException {
        throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "Update with SQL not yet supported.");
    }

    public void notifyMessage(final String nodeId, final Query query) {
        //do nothing
    }

    public void rollback() {
        //do nothing
    }
}
