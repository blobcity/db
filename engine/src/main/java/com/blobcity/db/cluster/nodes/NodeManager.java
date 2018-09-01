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

package com.blobcity.db.cluster.nodes;

import com.blobcity.db.cluster.ClusterNodesStore;
import com.blobcity.db.cluster.connection.ConnectionManager;
import com.blobcity.db.config.ConfigBean;
import com.blobcity.db.config.ConfigProperties;
import com.blobcity.db.constants.BSql;
import com.blobcity.db.constants.License;
import com.blobcity.db.exceptions.ErrorCode;
import com.blobcity.db.exceptions.OperationException;
import com.blobcity.util.json.JsonUtil;
import com.google.common.base.Preconditions;
import java.io.File;
import java.util.UUID;
import org.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 *
 * @author sanketsarang
 */
@Component
public class NodeManager {

    private static final Logger logger = LoggerFactory.getLogger(NodeManager.class);
    @Autowired
    private ConfigBean configBean;
    @Autowired
    private ClusterNodesStore clusterNodesStore;
    @Autowired
    private ProximityNodesStore proximityNodesStore;
    @Autowired
    private ConnectionManager connectionManager;

    public void notifiyConnection(String nodeId) {
        logger.info("Node connected: {}", nodeId);

        //TODO: Destroy all previous node connections if present
        //TODO: Activate re-connection protocol for the node
        //TODO: Open connections to the node inside ClusterNodesStore
    }

    public void notifyDisonnection(String nodeId) {
        logger.info("Node disconnected: {}", nodeId);

        //TODO: Unregister all nodes from ClusterNodesStore
        //TODO: Initiate node-restore service
    }

    public void addNode(final String nodeId) throws OperationException {
        if (!proximityNodesStore.contains(nodeId)) {
            throw new OperationException(ErrorCode.ADD_NODE_ERROR, nodeId + " could not be found by node discovery "
                    + "service. Use add-node command with ip specification instead.");
        }

        addNode(nodeId, proximityNodesStore.getIp(nodeId));
    }

    public void addNode(final String nodeId, final String ip) throws OperationException {
        Preconditions.checkNotNull(nodeId, new OperationException(ErrorCode.ADD_NODE_ERROR, "node-id cannot be null"));
        Preconditions.checkNotNull(ip, new OperationException(ErrorCode.ADD_NODE_ERROR, "ip address cannot be null"));

        JSONArray jsonArray = (JSONArray) configBean.getProperty(ConfigProperties.CLUSTER_NODES);
        if (JsonUtil.contains(jsonArray, nodeId)) {
            throw new OperationException(ErrorCode.ADD_NODE_ERROR, nodeId + " is already a member of the cluster");
        }

        /* Validate to check if connection to the respective node is possible */
        if (!connectionManager.validateConnection(nodeId, ip)) {
            throw new OperationException(ErrorCode.CLUSTER_ADD_NODE_VALIDATION_FAILED);
        }

        try {
            configBean.acquireExclusiveAccess();
            if (jsonArray == null) {
                jsonArray = new JSONArray();
            }
            jsonArray.put(nodeId);

            //TODO: Change to adding into a table inside the system_db database
            configBean.setProperty(ConfigProperties.CLUSTER_NODES, jsonArray);
            configBean.updateConfig();

            //TODO: Broadcast node addition so that all nodes can add the new node into the cluster
            clusterNodesStore.notifyAddNode(nodeId);
            connectionManager.connect(nodeId, ip);
        } catch (InterruptedException ex) {
            logger.error("Could not update configuration file with newly added cluster node with node-id: " + nodeId, ex);
            throw new OperationException(ErrorCode.CONFIG_FILE_ERROR, "Could not update configuration post add-node "
                    + "operation for newly added node with node-id: " + nodeId + ". Operation is rolled back");
        } finally {
            try {
                configBean.releaseExclusiveAccess();
            } catch (InterruptedException ex) {
                logger.error("Error releasing exclusive lock on configuration change. Cluster may require manual reboot", ex);
                throw new OperationException(ErrorCode.CONFIG_FILE_ERROR, "Could not release exclusive lock acquired on "
                        + "configuration file for performing add-node operation for adding node with node-id: " + nodeId);
            }
        }
    }

    public void removeNode(final String nodeId, final boolean graceful) throws OperationException {
        try {
            UUID.fromString(nodeId);
        } catch (IllegalArgumentException ex) {
            throw new OperationException(ErrorCode.INVALID_NODE_ID, nodeId + " is not a valid node id");
        }

        JSONArray jsonArray = (JSONArray) configBean.getProperty(ConfigProperties.CLUSTER_NODES);
        if (jsonArray == null) {
            throw new OperationException(ErrorCode.CONFIG_FILE_ERROR, "Configuration file does not have cluster "
                    + "nodes registry. The configuration may be corrupted and requires a manual restore.");
        }

        if (!JsonUtil.contains(jsonArray, nodeId)) {
            throw new OperationException(ErrorCode.ADD_NODE_ERROR, nodeId + " is not a member of the cluster");
        }

        //TODO: Perform remove node operation for disconnecting the node from the cluster. All nodes in the cluster must response with success for this operation
        try {

            configBean.acquireExclusiveAccess();
            for (int i = 0; i < jsonArray.length(); i++) {
                if (jsonArray.getString(i).equals(nodeId)) {
                    jsonArray.remove(i);
                    break;
                }
            }
            configBean.setProperty(ConfigProperties.CLUSTER_NODES, jsonArray);
            configBean.updateConfig();
            clusterNodesStore.notifyRemoveNode(nodeId);
        } catch (InterruptedException ex) {
            logger.error("Could not update configuration file for removing cluster node with node-id: " + nodeId, ex);
            throw new OperationException(ErrorCode.CONFIG_FILE_ERROR, "Could not update configuration post add-node operation for newly added node with node-id: " + nodeId);
        } finally {
            try {
                configBean.releaseExclusiveAccess();
            } catch (InterruptedException ex) {
                logger.error("Error releasing exclusive lock on configuration change", ex);
                throw new OperationException(ErrorCode.CONFIG_FILE_ERROR, "Could not release exclusive lock acquired"
                        + " on configuration file for performing remove-node operation for removing node with node-id: "
                        + nodeId);
            }
        }
    }

    public String getNodeIdFromIp(final String ip) {
        return null;
    }

    /**
     * Set's up the current running instance as a database node
     */
    public void setupNode() {
        File file;

        /* Create base directory */
        file = new File(BSql.BSQL_BASE_FOLDER);
        if (!file.exists()) {
            file.mkdir();
        }

        /* Create global-live directory */
        file = new File(BSql.GLOBAL_LIVE_FOLDER);
        if (!file.exists()) {
            file.mkdir();
        }

        /* Create global-del directory */
        file = new File(BSql.GLOBAL_DELETE_FOLDER);
        if (!file.exists()) {
            file.mkdir();
        }

        /* Create commit-logs directory */
        file = new File(BSql.COMMIT_LOGS_FOLDER);
        if(!file.exists()) {
            file.mkdir();
        }

        /* Create default configuration file */
        configBean.createDefaultConfig(BSql.STORAGE_VERSION);
    }

    public boolean isSetup() {
        if(new File(BSql.BSQL_BASE_FOLDER).exists()){
            // check if config file is present
            if(new File(BSql.CONFIF_FILE).exists()){
                // read contents and check if node-id is present or nor.
                String nodeId = configBean.getStringProperty("node-id");
                String version = configBean.getStringProperty("version");
                return nodeId != null &&  version != null;
            }
            return false;
        }
        return false;
    }
}
