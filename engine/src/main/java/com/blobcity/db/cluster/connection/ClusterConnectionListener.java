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

import com.blobcity.db.constants.Ports;
import java.net.Socket;
import javax.annotation.PostConstruct;
import org.springframework.stereotype.Component;

/**
 *
 * @author sanketsarang
 */
@Component
public class ClusterConnectionListener extends ConnectionEndpoint {

    public ClusterConnectionListener() {
        super(Ports.CLUSTER_PORT);
    }

    @PostConstruct
    private void init() {

        /* Starts a thread that listens to incoming TCP connections for inter node connection on cluster */
        start();
    }

    @Override
    protected void processNewClient(Socket socket) {
        //TODO: Start a new ClusterConnection and add it to the ClusterStore
    }
}
