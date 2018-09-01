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

package com.blobcity.db.master.executors.schema;

import com.blobcity.db.cluster.ClusterNodesStore;
import com.blobcity.db.master.MasterExecutable;
import com.blobcity.db.master.aggregators.UniqueAggregator;
import com.blobcity.db.master.executors.generic.ExecuteSelectedNodesReadMaster;
import com.blobcity.lib.query.Query;

import java.util.Arrays;
import java.util.HashSet;

/**
 * @author sanketsarang
 */
public class ListCollectionMaster extends ExecuteSelectedNodesReadMaster implements MasterExecutable {

    public ListCollectionMaster(Query query) {
        super(query, new HashSet<String>(Arrays.asList(new String[]{ClusterNodesStore.getInstance().getSelfId()})), new UniqueAggregator<String>());
    }
}
