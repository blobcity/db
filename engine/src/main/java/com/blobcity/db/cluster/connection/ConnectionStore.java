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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Semaphore;
import org.springframework.stereotype.Component;

/**
 * Stores a collection of {@link ClusterConnection} mapped to the respective node-id's of the nodes. It is
 * responsibility of the connection manager to ensure that this store at all times has only active connections listed.
 * This class is also not responsible for opening and closing the {@link ClusterConnection}.
 *
 * @author sanketsarang
 */
@Component
public class ConnectionStore {

    private final Random random = new Random();
    private final Map<String, List<ClusterConnection>> map = new HashMap<>();
    private final Map<String, List<ClusterConnection>> inUseMap = new HashMap<>();

    public void addConnection(final String nodeId, final ClusterConnection clusterConnection) {
        if (!map.containsKey(nodeId)) {
            map.put(nodeId, new ArrayList<ClusterConnection>());
        }

        map.get(nodeId).add(clusterConnection);
    }

    public void removeConnection(final String nodeId, final ClusterConnection clusterConnection) {
        if (!map.containsKey(nodeId)) {
            return;
        }

        map.get(nodeId).remove(clusterConnection);
    }

    public void removeNode(final String nodeId) {
        map.remove(nodeId);
    }

    public ClusterConnection getConnection(final String nodeId) {
        if (!map.containsKey(nodeId)) {
            return null;
        }

        List<ClusterConnection> connectionList = map.get(nodeId);
        return connectionList.get(random.nextInt(connectionList.size()));
    }
}
