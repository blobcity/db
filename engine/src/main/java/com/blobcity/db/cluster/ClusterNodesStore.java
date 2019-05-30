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

package com.blobcity.db.cluster;

import com.blobcity.db.config.ConfigBean;
import com.blobcity.db.config.ConfigProperties;
import com.blobcity.db.exceptions.OperationException;

import java.util.*;
import javax.annotation.PostConstruct;

//import com.blobcity.license.License;
import org.apache.mina.util.ConcurrentHashSet;
import org.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * This stores nodes that are members of the cluster.
 *
 * @author sanketsarang
 */
@Component
public class ClusterNodesStore {

    public static String selfId = null;
    private Set<String> clusterNodes = new ConcurrentHashSet<>();
    private Set<String> onlineNodes = new ConcurrentHashSet<>();
    private static ClusterNodesStore clusterBeanInstance;
    private static final Logger logger = LoggerFactory.getLogger(ClusterNodesStore.class.getName());

    @Autowired
    private ConfigBean configBean;

    @PostConstruct
    private void init() {
        clusterBeanInstance = this;
        clusterBeanInstance.loadClusterNodes();
        System.out.println("SelfId: " + selfId);
    }

    public static ClusterNodesStore getInstance() {
        return clusterBeanInstance;
    }

    public String getSelfId() {
        return selfId;
    }

    /**
     * Notifies the store of a new node addition to the cluster, and adds the node-id of the newly added node to the
     * cached list of connected nodes. This function is a no-op if the node-id is already cached in the store.
     *
     * @param nodeId the node-id of the node that was added to the cluster
     */
    public void notifyAddNode(final String nodeId) {
        clusterNodes.add(nodeId);
    }

    /**
     * Notifies the store of a node removal from the cluster. The function is a no-op if the node-id is not a member of
     * the stores cache.
     *
     * @param nodeId the node-id of the node that was removed from the cluster
     */
    public void notifyRemoveNode(final String nodeId) {
        clusterNodes.remove(nodeId);
    }

    public boolean hasNode(String nodeId) throws OperationException {
        return clusterNodes.contains(nodeId);
    }
    
    /**
     * Gets all the nodes currently part of the cluster
     * @return a {@link Set} of nodes currently part of the cluster
     */
    public Set<String> getAllNodes() {
        return Collections.unmodifiableSet(clusterNodes);
    }

    /**
     * Gets a collection of currently least loaded nodes. The number of nodes returned are expected to satisfy the
     * replication factor specified, but can be lesser than the replication factor if the required number of nodes
     * are not available.
     * @param replicationFactor the replication factor. 0 means no replication, and -1 means full replication for
     *                          mirrored type collections
     * @return a {@link Set} of node-id's belonging to nodes that are currently least loaded
     */
    public Set<String> getLeastLoadedNodes(int replicationFactor) {
        if(replicationFactor == -1 || clusterNodes.size() == 1) {
            return Collections.unmodifiableSet(clusterNodes);
        }

        //TODO: For no replication, improve to send data to least loaded nodes than sending to self node
        if(replicationFactor == 0) {
            return new HashSet<>(Arrays.asList(getSelfId()));
        }

        //TODO: implement the load factor based return of nodes
        throw new UnsupportedOperationException("not supported yet.");
    }

    private void loadClusterNodes() {
        Object obj = configBean.getProperty(ConfigProperties.CLUSTER_NODES);
        if (obj == null) {
            logger.warn("Could not find cluster configuration. Defaulting to single node mode. If this node is expected to "
                    + "be part of a cluster, you must shut down the node immediately to prevent data corruption and "
                    + "list the cluster configuration inside config.json property file before booting.");
        } else {
            JSONArray jsonArray = (JSONArray) configBean.getProperty(ConfigProperties.CLUSTER_NODES);
            clusterNodes.clear();
            for (int i = 0; i < jsonArray.length(); i++) {
                clusterNodes.add(jsonArray.getString(i));
            }
        }

//        String selfNodeId = configBean.getStringProperty(ConfigProperties.NODE_ID);
//        String selfNodeId = License.getNodeId();
        String selfNodeId = "default"; //temp code until removal of licensing module

        if (selfNodeId == null) {
            logger.warn("Self node id is not configured. Cluster may not function correctly");
        } else {
            clusterNodes.add(selfNodeId);
            this.selfId = selfNodeId;
        }
    }
}
