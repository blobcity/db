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

import com.blobcity.db.cluster.ClusterNodesStore;
import com.blobcity.db.cluster.messaging.messages.Message;
import com.blobcity.db.cluster.messaging.messages.NodeConnectionHeaderMessage;
import com.blobcity.db.constants.ClusterConstants;
import com.blobcity.db.exceptions.OperationException;
import com.blobcity.db.startup.StorageStartup;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Manages cluster connection and disconnection procedures
 *
 * @author sanketsarang
 */
@Component
public class ConnectionManager {

    private static final int CONNECTION_TIMEOUT = 5000; //value in milli-seconds
    private static final Logger logger = LoggerFactory.getLogger(StorageStartup.class);

    @Autowired
    private ClusterNodesStore clusterNodesStore;
    @Autowired
    private ConnectionStore connectionStore;

    public boolean validateConnection(final String nodeId, final String ipAddress) throws OperationException {
        logger.info("Validating connection to " + nodeId + " at ip " + ipAddress);

        Socket socket = null;
        try {
            try {
                socket = connect(ipAddress, ClusterConstants.CLUSTER_PORT);
            } catch (IOException ex) {
                logger.error("Failed to open cluster connection to ip " + ipAddress + " on port " + ClusterConstants.CLUSTER_PORT);
                return false;
            }

            /* Write nodeId of the self node so that the remote node understands who is connecting */
            final String selfNodeId = clusterNodesStore.getSelfId();
            try {
                writeConnectionHeader(socket, selfNodeId);
            } catch (IOException ex) {
                logger.error("Error writing connect-node command on connection to ip " + ipAddress + " on port "
                        + ClusterConstants.CLUSTER_PORT + ". The connection will now be closed.");
                return false;
            }

            /* Read nodeId of remote node and ensure that is same as the node to which the connection is being opened */
            final String remoteNodeId;
            try {
                remoteNodeId = readConnectionHeader(socket);

                /* If actual connected node id does not match expected node id, then the connection must be disconnected */
                if (!nodeId.equals(remoteNodeId)) {
                    logger.error("Cluster connection validation established to incorrect node or the node ip addresses "
                            + "has changed. Expected node id " + nodeId + " at ip " + ipAddress + " but node with nodeId "
                            + remoteNodeId + " found. The connection will be disconnected.");
                    return false;
                }
            } catch (IOException ex) {
                logger.error("Error reading connect-node command on connection to ip " + ipAddress + " on port "
                        + ClusterConstants.CLUSTER_PORT + ". The connection will now be closed.");
                return false;
            }

            return true;
        } finally {
            if (socket != null && socket.isConnected()) {
                try {
                    socket.close();
                } catch (IOException ex) {
                    logger.error(ex.getMessage());
                }
            }
        }
    }

    @Async
    public void connect(final String nodeId, final String ipAddress) throws OperationException {
        logger.info("Attempting connection to " + nodeId + " at ip " + ipAddress);

        Socket socket = null;
        try {
            socket = connect(ipAddress, ClusterConstants.CLUSTER_PORT);
        } catch (IOException ex) {
            if (socket != null && socket.isConnected()) {
                try {
                    socket.close();
                } catch (IOException ex1) {
                    //do nothing
                }
            }
            logger.error("Failed to open cluster connection to ip " + ipAddress + " on port " + ClusterConstants.CLUSTER_PORT);
            return;
        }

        /* Write nodeId of the self node so that the remote node understands who is connecting */
        final String selfNodeId = clusterNodesStore.getSelfId();
        try {
            writeConnectionHeader(socket, selfNodeId);
        } catch (IOException ex) {
            logger.error("Error writing connect-node command on connection to ip " + ipAddress + " on port "
                    + ClusterConstants.CLUSTER_PORT + ". The connection will now be closed.");
            try {
                socket.close();
            } catch (IOException ex1) {
                logger.error(ex1.getMessage(), ex1);
            }
            return;
        }

        /* Read nodeId of remote node and ensure that is same as the node to which the connection is being opened */
        final String remoteNodeId;
        try {
            remoteNodeId = readConnectionHeader(socket);

            /* If actual connected node id does not match expected node id, then the connection must be disconnected */
            if (!nodeId.equals(remoteNodeId)) {
                logger.error("Cluster connection established to incorrect node or the node ip addresses has changed. "
                        + "Expected node id " + nodeId + " at ip " + ipAddress + " but node with nodeId "
                        + remoteNodeId + " found. The connection will be disconnected.");
                try {
                    socket.close();
                } catch (IOException ex) {
                    logger.error(ex.getMessage(), ex);
                }
            }
        } catch (IOException ex) {
            logger.error("Error reading connect-node command on connection to ip " + ipAddress + " on port "
                    + ClusterConstants.CLUSTER_PORT + ". The connection will now be closed.");
            try {
                socket.close();
            } catch (IOException ex1) {
                logger.error(ex1.getMessage(), ex1);
            }
            return;
        }

        /* Create a new connection pool */
        for (int i = 0; i < ClusterConstants.DEFAULT_CONNECTION_POOL; i++) {
            ClusterConnection clusterConnection = new ClusterConnection(socket, selfNodeId, remoteNodeId);
            connectionStore.addConnection(remoteNodeId, clusterConnection);
        }

        logger.info("Cluster connection successfully opened to ip " + ipAddress + " on port " + ClusterConstants.CLUSTER_PORT);
    }

    /**
     * Opens a new socket connection on the specified IP address on the specified port. This function is a blocking call
     * until the connection is established.
     *
     * @param ipAddress the IP address to connect to
     * @param port the port on which the socket is to be opened
     * @return a connected instance of {@link Socket}
     * @throws IOException if an error occurs during the connection
     * @throws SocketTimeoutException if timeout occurs before the connection is set
     */
    private Socket connect(final String ipAddress, final int port) throws IOException, SocketTimeoutException {
        Socket socket = new Socket();
        SocketAddress socketAddress = new InetSocketAddress(ipAddress, port);
        socket.connect(socketAddress, CONNECTION_TIMEOUT);
        return socket;
    }

    /**
     * Writes the NodeConnectionHeaderMessagenHeader} {@link Message} on the specified socket
     *
     * @param socket the socket on which the cluster command is to be written
     * @param selfNodeId the self node id of the current node, used NodeConnectionHeaderMessagennectionHeader}
     * @throws IOException if an error occurs in writing the message
     */
    private void writeConnectionHeader(final Socket socket, final String selfNodeId) throws IOException {
        Message message = new NodeConnectionHeaderMessage(selfNodeId);
        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()))) {
            writer.write(message.toString());
            writer.newLine();
            writer.flush();
        }
    }

    /**
     * NodeConnectionHeaderMessage {@link NodeConnectionHeader} {@link Message} on the specified socket
     *
     * @param socket the socket on which the cluster command is to be read
     * @return the nodeId of the remote node on which the socket is connected
     * @throws IOException if an error occurs in reading the message
     * @throws OperationException if message parsing failed
     */
    private String readConnectionHeader(final Socket socket) throws IOException, OperationException {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
            String message = reader.readLine();
            NodeConnectionHeaderMessage nodeConnectionHeaderMessage = new NodeConnectionHeaderMessage();
            nodeConnectionHeaderMessage.init(message);
            return nodeConnectionHeaderMessage.getNodeId();
        }
    }
}
