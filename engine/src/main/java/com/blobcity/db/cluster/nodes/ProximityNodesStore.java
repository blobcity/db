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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * This class only stores live snapshots of nodes and is used to identify and store information of nodes in the vicinity
 * of the current node. Nodes listed inside the node store are not necessarily part of the cluster, but if the node
 * store does not have a node listed that is a cluster member, then it is indicative of the respective node most likely
 * being offline.
 *
 * @author sanketsarang
 */
@Component
public class ProximityNodesStore {

    private final long KILL_DURATION = 3000;//milliseconds

    /* node id -> node ip map */
    private final Map<String, String> nodeMap = new ConcurrentHashMap<>();
    private final Map<String, Long> lastBeacon = new ConcurrentHashMap<>();
    @Autowired
    private NodeManager nodeManager;

    public void add(String nodeId, String ip) {
        nodeMap.put(nodeId, ip);
        notifyBeacon(nodeId);
        nodeManager.notifiyConnection(nodeId);
    }

    public void remove(String nodeId) {
        nodeMap.remove(nodeId);
        lastBeacon.remove(nodeId);
        nodeManager.notifyDisonnection(nodeId);
    }

    public boolean contains(String nodeId) {
        return nodeMap.containsKey(nodeId);
    }
    
    /**
     * Gets the IP address of the specified node. Caller must perform <code>NodeStore#contains</code> check.
     * @param nodeId the node-id of the node
     * @return ip address of the node if node is listed inside {@link ProximityNodesStore}; <code>null</code> otherwise
     */
    public String getIp(String nodeId) {
        return nodeMap.get(nodeId);
    }

    public void notifyBeacon(String nodeId) {
        lastBeacon.put(nodeId, System.currentTimeMillis());
    }

    public void notifyBeacon(String nodeId, String ip) {
        if (!nodeMap.containsKey(nodeId)) {
            add(nodeId, ip);
        } else if (!nodeMap.get(nodeId).equals(ip)) {
            remove(nodeId);
            add(nodeId, ip);
        }

        lastBeacon.put(nodeId, System.currentTimeMillis());
    }

    public Set<String> listNodes() {
        return nodeMap.keySet();
    }

    @Scheduled(fixedRate = 333)
    public void deadNodeCheck() {
        final long currentTime = System.currentTimeMillis();

        new HashSet<>(lastBeacon.entrySet()).stream().filter((entry) -> (currentTime - entry.getValue() > KILL_DURATION)).map((entry) -> {
            remove(entry.getKey());
            return entry;
        }).forEach((entry) -> {
            LoggerFactory.getLogger(ProximityNodesStore.class.getName()).error("Node {} disconnected from cluster", entry.getKey());
        });
    }
}
