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

package com.blobcity.db.cluster.connection;

import com.blobcity.db.cluster.messaging.messages.Message;
import com.blobcity.db.exceptions.ErrorCode;
import com.blobcity.db.exceptions.OperationException;
import com.blobcity.db.processors.ProcessHandler;
import com.blobcity.db.processors.ProcessorExecutorService;
import com.blobcity.lib.query.Query;
import com.blobcity.util.json.JsonMessages;
import java.io.IOException;
import java.net.Socket;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents a single cluster connection, connecting two nodes
 *
 * @author sanketsarang
 */
public class ClusterConnection extends TcpConnectionClient {

    private static final Logger logger = LoggerFactory.getLogger(ClusterConnection.class.getName());

    private final String selfNodeId;
    private final String remoteNodeId;

    public ClusterConnection(Socket socket, String selfNodeId, String remoteNodeId) {
        super(socket);
        this.selfNodeId = selfNodeId;
        this.remoteNodeId = remoteNodeId;
    }

    public String getSelfNodeId() {
        return selfNodeId;
    }

    public String getRemoteNodeId() {
        return remoteNodeId;
    }

    public void sendMessage(final String requestId, final String type, final JSONObject payloadJson) throws OperationException {
        try {
            writeMessage(JsonMessages.clusterCommandRequestMessage(requestId, selfNodeId, type, payloadJson).toString());
        } catch (IOException ex) {
            logger.error("Cluster communication error", ex);
            throw new OperationException(ErrorCode.CLUSTER_CONNECTION_ERROR);
        }
    }

    public void sendMessage(Message message) throws OperationException {
        try {
            writeMessage(message.toString());
        } catch (IOException ex) {
            logger.error("Cluster communication error", ex);
            throw new OperationException(ErrorCode.CLUSTER_CONNECTION_ERROR);
        }
    }

    public void sendSuccessMessage(final String requestId, final String type) throws OperationException {
        try {
            writeMessage(JsonMessages.clusterCommandResponseAck(requestId, selfNodeId, type, true).toString());
        } catch (IOException ex) {
            logger.error("Cluster communication error", ex);
            throw new OperationException(ErrorCode.CLUSTER_CONNECTION_ERROR);
        }
    }

    public void sendFailureMessage(final String requestId, final String type) throws OperationException {
        try {
            writeMessage(JsonMessages.clusterCommandResponseAck(requestId, selfNodeId, type, false).toString());
        } catch (IOException ex) {
            logger.error("Cluster communication error", ex);
            throw new OperationException(ErrorCode.CLUSTER_CONNECTION_ERROR);
        }
    }

    /**
     * Method is called when a new message is received from the remote node
     *
     * @param message the received message
     */
    @Override
    protected void processMessage(String message) {

        ProcessHandler processHandler = new ProcessHandler(remoteNodeId, new Query(new JSONObject(message)));
        ProcessorExecutorService.getInstance().submit(processHandler);


//        RequestProcessor requestExecutor = applicationContext.getBean(RequestProcessor.class);
//        try {
//            requestExecutor.processMessage(message);
//            return JsonMessages.SUCCESS_ACKNOWLEDGEMENT;
//        } catch (OperationException ex) {
//            return JsonMessages.ERROR_ACKNOWLEDGEMENT;
//        }
    }
}
