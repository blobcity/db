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

package com.blobcity.db.master.executors.generic;

import com.blobcity.db.master.AbstractCommitMaster;
import com.blobcity.db.master.MasterExecutable;
import com.blobcity.lib.query.Query;

import java.util.Set;

/**
 * Executes the specified command on selected nodes. The nodes can be explicitely specified, or the master can be
 * allowed to choose nodes based on its own strategy. When auto choosing nodes, the number of nodes on which the
 * query will execute is based on the replication factor settings of the cluster.
 *
 * @author sanketsarang
 */
public class ExecuteSelectedNodesCommitMaster extends AbstractCommitMaster implements MasterExecutable {

    public ExecuteSelectedNodesCommitMaster(final Query query, final Set<String> nodeIds) {
        super(query, nodeIds);
    }

    public ExecuteSelectedNodesCommitMaster(final Query query) {
        super(query);

        //TODO: Select nodes based on replication strategy

        throw new UnsupportedOperationException("Not supported yet.");
    }
}
