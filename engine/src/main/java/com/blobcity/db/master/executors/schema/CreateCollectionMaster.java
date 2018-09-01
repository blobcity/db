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
import com.blobcity.db.license.LicenseRules;
import com.blobcity.db.master.MasterExecutable;
import com.blobcity.db.master.executors.generic.ExecuteAllNodesCommitMaster;
import com.blobcity.lib.query.Query;
import com.blobcity.lib.query.QueryParams;
import com.blobcity.lib.query.QueryType;
import org.springframework.expression.Operation;

/**
 * Master that manages the create-collection request
 *
 * @author sanketsarang
 */
public class CreateCollectionMaster extends ExecuteAllNodesCommitMaster implements MasterExecutable {

    public CreateCollectionMaster(Query query) throws OperationException {
        super(query);

        if(!LicenseRules.MEMORY_DURABLE_TABLES && query.getString(QueryParams.STORAGE_TYPE).equalsIgnoreCase("in-memory")) {
            throw new OperationException(ErrorCode.FEATURE_RESTRICTED, "Feature not available in current license. Consider purchasing an Enterprise License");
        }

        if(!LicenseRules.MEMORY_NON_DURABLE_TABLES &&
                (query.getString(QueryParams.STORAGE_TYPE).equalsIgnoreCase("in-memory-nd") || query.getString(QueryParams.STORAGE_TYPE).equalsIgnoreCase("in-memory-non-durable"))) {
            throw new OperationException(ErrorCode.FEATURE_RESTRICTED, "Feature not available in current license. Consider purchasing an Enterprise License");
        }

        if(query.getQueryType() != QueryType.CREATE_COLLECTION && query.getQueryType() != QueryType.CREATE_TABLE) {
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "Incorrect query passed to create-collection master");
        }
    }
}
