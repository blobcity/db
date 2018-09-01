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

package com.blobcity.db.cluster.messaging;

import com.blobcity.db.cluster.ClusterNodesStore;
import com.blobcity.db.cluster.connection.ClusterConnection;
import com.blobcity.db.cluster.connection.ConnectionStore;
import com.blobcity.db.exceptions.OperationException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.blobcity.db.processors.ProcessHandler;
import com.blobcity.db.processors.ProcessorExecutorService;
import com.blobcity.lib.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

/**
 * Handles inter node communication on the cluster.
 *
 * @author sanketsarang
 */
@Component
public class ClusterMessaging {

    private static final Logger logger = LoggerFactory.getLogger(ClusterMessaging.class.getName());

    @Autowired @Lazy
    private ClusterNodesStore clusterNodesStore;
    @Autowired @Lazy
    private ConnectionStore connectionStore;

    /**
     * Sends a message to all nodes on the cluster, and returns back response of every node
     *
     * @param query the message to be broadcasted
     * @return a {@link Map} keyed on node-id with value containing response of that node in String form,
     * <code>null</code> if the specific node could not be reached
     */
    public Map<String, Boolean> sendMessage(Query query) {
        Map<String, Boolean> sendStatusMap = new HashMap<>();
        Set<String> nodeIds = clusterNodesStore.getAllNodes();
        nodeIds.parallelStream().forEach((nodeId) -> {
            boolean success = sendMessage(query, nodeId);
            sendStatusMap.put(nodeId, success);
        });

        return sendStatusMap;
    }

    /**
     * Sends a message to a single node on the cluster, and returns back the response acquired from that node
     *
     * @param query the message to be broadcasted
     * @param nodeId the node id of the node to which the message is sent
     * @return <code>true</code> if message sending was successful; <code>false</code> otherwise
     */
    public boolean sendMessage(Query query, String nodeId) {
        logger.debug("Send Cluster Message from: {}, to: {}, message: {}", clusterNodesStore.getSelfId(), nodeId, query.toJsonString());

        query.fromNode(clusterNodesStore.getSelfId()); //set the from_node_id to the current node

        /* Short circuit communication layer for self-node */
        if (nodeId.equals(clusterNodesStore.getSelfId())) {
            ProcessHandler processHandler = new ProcessHandler(clusterNodesStore.getSelfId(), query);
            ProcessorExecutorService.getInstance().submit(processHandler);
            return true;
        }

        ClusterConnection clusterConnection = connectionStore.getConnection(nodeId);
        if (clusterConnection == null) {
            return false;
        }
        try {
            clusterConnection.sendMessage(query.getRequestId(), query.getQueryType().getQueryCode(), query.toJson());
            return true;
        } catch (OperationException ex) {
            logger.error(ex.getMessage(), ex);
            return false;
        }
    }

    /**
     * Sends a message to the specified nodes
     *
     * @param query the message to be sent
     * @param nodeIds the nodes to which the message is to be sent
     * @return {@link Map} containing success / failure status of sending to each of the nodes
     */
    public Map<String, Boolean> sendMessage(Query query, Set<String> nodeIds) {
        Map<String, Boolean> sendStatusMap = new HashMap<>();
        nodeIds.parallelStream().forEach((nodeId) -> {
            boolean success = sendMessage(query, nodeId);
            sendStatusMap.put(nodeId, success);
        });

        return sendStatusMap;
    }
}
