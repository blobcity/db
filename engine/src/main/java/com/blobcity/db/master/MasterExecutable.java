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

package com.blobcity.db.master;

import com.blobcity.db.cluster.messaging.messages.Message;
import com.blobcity.db.exceptions.OperationException;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import com.blobcity.lib.query.Query;
import org.json.JSONObject;
import org.springframework.scheduling.annotation.AsyncResult;

/**
 *
 * @author sanketsarang
 */
public interface MasterExecutable extends Callable<Query>{

    /**
     * Notifies of a message received for accumulation process
     *
     * @param nodeId the node-id of the node from which the message originated
     * @param query the query
     */
    public void notifyMessage(final String nodeId, Query query);

    /**
     * Used to externally initiate a rollback on the request
     */
    public void rollback();
}
