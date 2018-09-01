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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.Socket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Will handle the socket to read and write from a connected TCP stream. This class must be instantiated only an a
 * connected socket.
 *
 * @author sanketsarang
 */
public abstract class TcpConnectionClient extends Thread {

    private static Logger logger = LoggerFactory.getLogger(TcpConnectionClient.class.getName());

    private Socket socket;

    public TcpConnectionClient(Socket socket) {
        this.socket = socket;
    }

    /**
     * Will continue listening for incoming messages till socket is connected. All received messages are forwarded to
     * the processMessage(...) function.
     */
    @Override
    public void run() {
        BufferedReader reader = null;
        String message;
        try {
            reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));

            while ((message = reader.readLine()) != null) {
                processMessage(message);
            }
        } catch (Exception ex) {
            logger.error("Dropped socket connection", ex);
            //LoggerFactory.getLogger(TcpConnectionClient.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            try {
                reader.close();
                socket.close();
            } catch (IOException ex) {
                //LoggerFactory.getLogger(TcpConnectionClient.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

    public boolean isConnected() {
        return socket.isConnected();
    }

    public void disconnect() throws IOException {
        socket.close();
    }

    /**
     * Writes a TCP message on the socket
     *
     * @param message The message to be written
     * @throws IOException
     */
    protected void writeMessage(String message) throws IOException {
        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()))) {
            writer.write(message);
            writer.newLine();
            writer.flush();
        }
    }

    /**
     * Abstract function to be implemented by the sub class to process the received message. This function must return a
     * null if nothing is to be written back on the output stream.
     *
     * @param message Message to be processed
     * @return Result of the processed message to be written back to the output stream
     */
    protected abstract void processMessage(String message);
}
