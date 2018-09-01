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

import com.blobcity.db.constants.ClusterConstants;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;
import org.slf4j.LoggerFactory;

/**
 *
 * @author sanketsarang
 */
public class BeaconListener extends Thread {

    private DatagramSocket socket;
    private boolean keepRunning = true;
    private boolean failureLogged = false;
    private final NodeDiscovery nodeDiscovery;
    
    public BeaconListener(NodeDiscovery nodeDiscovery) {
        this.nodeDiscovery = nodeDiscovery;
    }

    @Override
    public void run() {
        try {
            DatagramPacket packet;
            socket = new DatagramSocket(ClusterConstants.BEACON_PORT);
            packet = new DatagramPacket(new byte[51], 51);
            while (keepRunning) {
                try {
                    socket.receive(packet);
                    final String ip = packet.getAddress().toString();
                    String nodeId = new String(packet.getData());
                    nodeDiscovery.notifyBeacon(nodeId, ip);
                    failureLogged = false;
                } catch (IOException ie) {
                    if (!failureLogged) {
                        LoggerFactory.getLogger(BeaconListener.class.getName()).error(null, ie);
                        failureLogged = true;
                    }
                }
            }
        } catch (SocketException ex) {
            LoggerFactory.getLogger(BeaconListener.class.getName()).error(null, ex);
        }
    }

    public void close() {
        socket.close();
    }

    public void killThread() {
        keepRunning = false;
    }
}
