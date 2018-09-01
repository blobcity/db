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

import com.blobcity.db.cluster.ClusterNodesStore;
import com.blobcity.db.master.MasterExecutable;
import com.blobcity.db.master.MasterStore;
import com.blobcity.db.cluster.messaging.ClusterMessaging;
import com.blobcity.db.cluster.messaging.messages.JsonQueryMessage;
import com.blobcity.db.cluster.messaging.messages.JsonResultSetMessage;
import com.blobcity.db.cluster.messaging.messages.Message;
import com.blobcity.db.cluster.messaging.messages.MessageParser;
import com.blobcity.db.exceptions.OperationException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

/**
 * <p>
 * Processes requests received from the clustering layer and passes them on for execution to the appropriate request
 * processors. This is a central point that takes both JSON and SQL format for the queries.
 *
 * <p>
 * Possible values of type (<code>typ</code>) parameter:
 * <ul>
 * <li><code>e</code>: Soft Execute</li>
 * <li><code>c</code>: Commit</li>
 * <li><code>r</code>: Roll-back</li>
 * </ul>
 *
 * @author sanketsarang
 */
@Component
@Deprecated
public class RequestProcessor {

    @Autowired @Lazy
    private ClusterNodesStore clusterNodesStore;
    @Autowired @Lazy
    private InternalBQueryExecutor internalBQueryExecutor;
    @Autowired @Lazy
    private MasterStore masterStore;
    @Autowired @Lazy
    private ClusterMessaging clusterMessaging;

    public void processMessage(String messageString) throws OperationException {
        processMessage(MessageParser.parse(messageString));
    }

    public void processMessage(Message message) throws OperationException {
//        if (message.isRequest()) {
//            processRequestMessage(message);
//        } else {
//            processResponseMessage(message);
//        }
    }

    private void processRequestMessage(Message requestMessage) {
//        Message responseMessage = null;
//
//
//        switch (requestMessage.getMessageType()) {
//            case JSON_QUERY:
//                responseMessage = processJsonQueryMessage((JsonQueryMessage) requestMessage);
//                break;
//        }
//
//        if (responseMessage == null) {
//            return;
//        }
//
//        if (responseMessage.getTargetNodeId() == null) {
//            clusterMessaging.sendMessage(responseMessage);
//        } else {
//            clusterMessaging.sendMessage(responseMessage, responseMessage.getTargetNodeId());
//        }
    }

    private void processResponseMessage(Message message) throws OperationException {
//        switch (message.getMessageType()) {
//            case JSON_RESULT_SET:
//                processJsonResultSetMessage((JsonResultSetMessage) message);
//                break;
//        }
    }

    private JsonResultSetMessage processJsonQueryMessage(JsonQueryMessage jsonQueryMessage) {
        return null;
//        JsonResultSetMessage jsonResultSetMessage;
//        JSONObject responseJson;
//        try {
//            responseJson = internalBQueryExecutor.executeQuery(jsonQueryMessage.getQueryJson());
//        } catch (OperationException ex) {
//            responseJson = createErrorMessage(ex);
//        }
//
//        /* Populate response message */
//        jsonResultSetMessage = new JsonResultSetMessage();
//        jsonResultSetMessage.init(jsonQueryMessage);
//        jsonResultSetMessage.setJsonResult(responseJson);
//        jsonResultSetMessage.setPage(1);
//        jsonResultSetMessage.setPages(1);
//        jsonResultSetMessage.setSenderNodeId(clusterNodesStore.getSelfId());
//        jsonResultSetMessage.setTargetNodeId(jsonQueryMessage.getMasterNodeId());
//        return jsonResultSetMessage;
    }

    private void processJsonResultSetMessage(JsonResultSetMessage jsonResultSetMessage) throws OperationException {
//        MasterExecutable masterExecutable = masterStore.get(jsonResultSetMessage.getRequestId());
//        masterExecutable.notifyMessage(jsonResultSetMessage.getSenderNodeId(), jsonResultSetMessage);
    }

    private JSONObject createErrorMessage(OperationException ex) {
        return null;
//        JSONObject jsonObject = new JSONObject();
//        jsonObject.put("ack", "0");
//        jsonObject.put("code", ex.getErrorCode().getErrorCode());
//        jsonObject.put("msg", ex.getErrorCode().getErrorMessage());
//        return jsonObject;
    }
}
