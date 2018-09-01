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

package com.blobcity.db.processors.schema;

import com.blobcity.db.bsql.BSqlDatastoreManager;
import com.blobcity.db.exceptions.OperationException;
import com.blobcity.db.processors.AbstractCommitProcessor;
import com.blobcity.db.processors.AbstractReadProcessor;
import com.blobcity.db.processors.Processor;
import com.blobcity.db.transaction.CentralCommitLogWriter;
import com.blobcity.lib.query.Query;
import com.blobcity.lib.query.QueryParams;

import java.util.List;

/**
 * @author sanketsarang
 */
public class ListDsProcessor extends AbstractReadProcessor implements Processor {

    public ListDsProcessor(Query query) {
        super(query);
    }

    @Override
    public void softCommit() {
        Query responseQuery;

        BSqlDatastoreManager datastoreManager = super.getBean(BSqlDatastoreManager.class);
        List<String> datastoreList = datastoreManager.listDatabases();

        responseQuery = new Query().requestId(query.getRequestId()).responseQuery().payload(datastoreList).ackSuccess();
        super.getClusterMessagingBean().sendMessage(responseQuery, query.getMasterNodeId());
    }

    @Override
    public void commit() {
        throw new IllegalStateException("Commit should not be called for read-only process operations");
    }

    @Override
    public void rollback() {
        //TODO: Cancel an ongoing select operation
    }
}
