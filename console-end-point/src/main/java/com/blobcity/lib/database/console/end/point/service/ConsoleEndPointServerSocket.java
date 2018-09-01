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

package com.blobcity.lib.database.console.end.point.service;

import java.io.IOException;
import java.net.ServerSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Opens a {@link ServerSocket} on the designated port for listening to incoming TCP connection requests. For every new
 * connection received a new {@link ConsoleEndPointService} is started.
 *
 * @author sanketsarang
 */
public class ConsoleEndPointServerSocket implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(ConsoleEndPointServerSocket.class);
    private final int port;

    public ConsoleEndPointServerSocket(final int port) {
        this.port = port;
    }

    @Override
    public void run() {
        ServerSocket serverSocket;
        try {
            serverSocket = new ServerSocket(port);
        } catch (IOException ex) {
            logger.error("Could not open listener on port {}. CLI console start failed.", port);
            return;
        }

        try {
            while (true) {
                new Thread(new ConsoleEndPointService(serverSocket.accept())).start();
            }
        } catch (IOException ex) {
            logger.error("CLI connection listener on port {} stopped", port);
        }
    }
}
