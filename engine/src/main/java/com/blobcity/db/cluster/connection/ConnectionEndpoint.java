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

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;
import org.slf4j.LoggerFactory;

/**
 *
 * @author sanketsarang
 */
public abstract class ConnectionEndpoint extends Thread {

    private ServerSocket serverSocket;

    /**
     * Used to set the port and bind the server socket to the specified port
     *
     * @param port port on which to bind the ServerSocket
     */
    public ConnectionEndpoint(int port) {
        try {
            serverSocket = new ServerSocket(port);
        } catch (IOException ex) {
            LoggerFactory.getLogger(ConnectionEndpoint.class.getName()).error(null, ex);
        }
    }

    /**
     * This function is responsible for listening to incoming client connection. The socket of a newly connected client is passed to the processNewClient
     * function.
     */
    @Override
    public void run() {
        try {
            while (true) {
                Socket socket = serverSocket.accept();
                processNewClient(socket);
            }
        } catch (Exception ex) {
            LoggerFactory.getLogger(ConnectionEndpoint.class.getName()).error(null, ex);
        }
    }

    /**
     * Writes a TCP message on the socket provided
     *
     * @param socket The socket on which the write operation is to be performed
     * @param message The actual message to be written on the socket
     * @throws IOException
     */
    protected void writeMessage(Socket socket, String message) throws IOException {
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
        writer.write(message);
        writer.newLine();
        writer.flush();
    }

    /**
     * Used to stop the server socket and terminate the thread
     *
     * @throws IOException
     */
    protected void stopListening() throws IOException {
        serverSocket.close();
    }

    /**
     * Tells whether the server socket is bound to a port or not
     *
     * @return Returns true if the ServerSocket is bound
     */
    public boolean isBound() {
        return serverSocket.isBound();
    }

    protected abstract void processNewClient(Socket socket);
}
