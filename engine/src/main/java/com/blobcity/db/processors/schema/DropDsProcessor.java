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
import com.blobcity.db.processors.Processor;
import com.blobcity.db.transaction.CentralCommitLogWriter;
import com.blobcity.lib.query.Query;
import com.blobcity.lib.query.QueryParams;

/**
 * @author sanketsarang
 */
public class DropDsProcessor extends AbstractCommitProcessor implements Processor {

    public DropDsProcessor(final Query query) {
        super(query);
    }

    @Override
    public void softCommit() {
        Query responseQuery;
        CentralCommitLogWriter commitLogWriter = super.getBean(CentralCommitLogWriter.class);

        commitLogWriter.write(query);

        final String ds = super.query.getString(QueryParams.DATASTORE);
        final String archiveCode = super.query.getString(QueryParams.ARCHIVE_CODE);
        BSqlDatastoreManager datastoreManager = super.getBean(BSqlDatastoreManager.class);

        int attempts = 0;
        while(attempts++ < 3) {
            if (super.transientStore.acquireDsPermit(ds)) {
                try {
                    datastoreManager.dropDatabase(ds, archiveCode);
                    setRollbackNeedsAction();
                    responseQuery = new Query().requestId(query.getRequestId()).softCommitSuccessQuery().ack("1");
                    super.getClusterMessagingBean().sendMessage(responseQuery, query.getMasterNodeId());
                    commitLogWriter.write(responseQuery);
                } catch (OperationException ex) {
                    responseQuery = new Query().requestId(query.getRequestId()).softCommitSuccessQuery().ack("0").errorCode(ex.getErrorCode().getErrorCode());
                    super.getClusterMessagingBean().sendMessage(responseQuery, query.getMasterNodeId());
                    commitLogWriter.write(responseQuery);
                }
                return;
            } else {
                //retry after 1 second
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    //do nothing
                }
            }
        }

        //TODO: Appropriately write action to commit logs
        responseQuery = new Query().requestId(query.getRequestId()).softCommitSuccessQuery().ack("0");
        super.getClusterMessagingBean().sendMessage(responseQuery, query.getMasterNodeId());
        commitLogWriter.write(responseQuery);
    }

    @Override
    public void commit() {
        super.transientStore.releaseDsPermit(query.getString(QueryParams.DATASTORE));

        Query responseQuery = new Query().requestId(query.getRequestId()).commitSuccessQuery().ack("1");
        super.getBean(CentralCommitLogWriter.class).write(responseQuery);
        super.getClusterMessagingBean().sendMessage(responseQuery, query.getMasterNodeId());
    }

    @Override
    public void rollback() {
        Query responseQuery;

        try {
            if(doesRollbackNeedAction()) {
                super.getBean(BSqlDatastoreManager.class).undoDropDs(super.query.getString(QueryParams.ARCHIVE_CODE));
            }

            responseQuery = new Query().requestId(query.getRequestId()).rollbackSuccessQuery().ack("1");
            super.getBean(CentralCommitLogWriter.class).write(responseQuery);
            super.getClusterMessagingBean().sendMessage(responseQuery, query.getMasterNodeId());
        }catch(OperationException ex) {
            responseQuery = new Query().requestId(query.getRequestId()).rollbackSuccessQuery().ack("0");
            super.getBean(CentralCommitLogWriter.class).write(responseQuery);
            super.getClusterMessagingBean().sendMessage(responseQuery, query.getMasterNodeId());
        }

        super.transientStore.releaseDsPermit(query.getString(QueryParams.DATASTORE));
    }
}
