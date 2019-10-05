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

import com.blobcity.db.config.ConfigBean;
import com.blobcity.db.config.ConfigProperties;
import com.blobcity.db.constants.ClusterConstants;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.scheduling.annotation.Scheduled;

/**
 *
 * @author sanketsarang
 */
@Component
public class NodeDiscovery {

    private InetAddress host;
    private DatagramSocket socket;
    private DatagramPacket packet;
    private BeaconListener beaconListener;
    @Autowired
    private ConfigBean configBean;
    @Autowired
    private ProximityNodesStore nodeStore;
    private boolean broadcastBeacon;
    private boolean broadcastBeaconAlreadyLogged;
    private static final Logger logger = LoggerFactory.getLogger(NodeDiscovery.class);

    @PostConstruct
    private void init() {
        
        if (!configBean.contains(ConfigProperties.CLUSTER_BROADCAST_IP)) {
            logger.warn("Broadcast IP not configured for node. Clustering will be disabled until an appropriate broadcast IP address is set.");
            return;
        }

        beaconListener = new BeaconListener(this);
        beaconListener.start();

        try {
            host = InetAddress.getByName(configBean.getStringProperty(ConfigProperties.CLUSTER_BROADCAST_IP));
            socket = new DatagramSocket();
            byte[] buff = configBean.getStringProperty(ConfigProperties.NODE_ID).getBytes();
            packet = new DatagramPacket(buff, buff.length, host, ClusterConstants.BEACON_PORT);
            broadcastBeacon = true;
            broadcastBeaconAlreadyLogged = false;
        } catch (UnknownHostException | SocketException ex) {
            logger.error("Unable to start node discovery", ex);
        }
    }

    @Scheduled(fixedRate = 1000)
    private void broadcastBeacon() {

        if (socket == null || packet == null) {
            logger.info("Beacon not initialized for broadcast. Attempting restart");
            init();
            return;
        }

        if (!broadcastBeacon) {
            if (!broadcastBeaconAlreadyLogged) {
                logger.info("Node discovery service for clustering is cancelled");
                broadcastBeaconAlreadyLogged = true;
            } else {
                logger.trace("Node discovery service for clustering is cancelled");
            }
            return;
        }

        try {
            socket.send(packet);
        } catch (IOException ex) {
            logger.warn("Node discovery service for clustering is cancelled. You need to manually add nodes");
            broadcastBeacon = false;
        }
    }

    @PreDestroy
    private void destroy() {
        beaconListener.close();
        beaconListener.killThread();
    }

    public void notifyBeacon(String nodeId, String ip) {
        nodeStore.notifyBeacon(nodeId, ip);
    }
}
