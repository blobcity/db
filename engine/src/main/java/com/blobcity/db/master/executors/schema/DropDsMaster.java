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

import com.blobcity.db.exceptions.ErrorCode;
import com.blobcity.db.exceptions.OperationException;
import com.blobcity.db.master.MasterExecutable;
import com.blobcity.db.master.executors.generic.ExecuteAllNodesCommitMaster;
import com.blobcity.lib.query.Query;
import com.blobcity.lib.query.QueryParams;
import com.blobcity.lib.query.QueryType;

import java.util.UUID;

/**
 * Acts as a master of drop-ds operation
 *
 * @author sanketsarang
 */
public class DropDsMaster extends ExecuteAllNodesCommitMaster implements MasterExecutable {

    final String archiveCode;

    public DropDsMaster(Query query) throws OperationException {
        super(query);

        if(query.getQueryType() != QueryType.DROP_DS && query.getQueryType() != QueryType.DROP_DB) {
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "Incorrect query passed to drop-ds master");
        }

        /* Create the archive code to which the existing data to be archived */
        this.archiveCode = UUID.randomUUID().toString();
        super.query.put(QueryParams.ARCHIVE_CODE, archiveCode);
    }

    @Override
    protected void complete(final Query query) {
        query.put(QueryParams.ARCHIVE_CODE, this.archiveCode);
        super.response = query;
        super.completed = true;
        super.semaphore.release();
    }
}
