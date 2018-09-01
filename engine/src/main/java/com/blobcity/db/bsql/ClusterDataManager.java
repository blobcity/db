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

package com.blobcity.db.bsql;

import com.blobcity.db.bquery.statements.*;
import com.blobcity.db.constants.BQueryParameters;
import com.blobcity.db.exceptions.OperationException;
import com.blobcity.db.schema.beans.SchemaStore;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

/**
 * This beans handles all the CRUD operations which require the support of flexible schema.
 * This layer is integrated with Clustering as well.
 *
 * @author Prikshit Kumar
 */
@Component
@Deprecated
public class ClusterDataManager {

    private static final Logger logger = LoggerFactory.getLogger(ClusterDataManager.class);

    @Autowired(required = false)
    @Lazy
    private BQueryInsertExecutor bQueryInsertExecutor;
    @Autowired(required = false)
    @Lazy
    private BQueryRemoveExecutor bQueryRemoveExecutor;
    @Autowired(required = false)
    @Lazy
    private BQuerySelectExecutor bQuerySelectExecutor;
    @Autowired(required = false)
    @Lazy
    private BQueryUpdateExecutor bQueryUpdateExecutor;


    public JSONObject select(JSONObject queryJSON) throws OperationException{
        return bQuerySelectExecutor.execute(queryJSON);
    }

    public JSONObject select(BQuerySelectStatement selectStatement) throws OperationException{
        return bQuerySelectExecutor.execute(selectStatement);
    }


    public JSONObject insert(JSONObject queryJSON) throws OperationException{
        return bQueryInsertExecutor.execute(queryJSON);
    }

    public JSONObject insert(BQueryInsertStatement insertStatement) throws OperationException{
        return bQueryInsertExecutor.execute(insertStatement);
    }

    public JSONObject insert(final String datastore, final String collection, final JSONObject rowJSON) throws OperationException{
        BQueryInsertStatement bQueryInsertStatement = new BQueryInsertStatement(datastore, collection,
                SchemaStore.getInstance().getSchema(datastore, collection).getPrimary(),
                rowJSON);
        return insert(bQueryInsertStatement);
    }


    public JSONObject update(JSONObject queryJSON) throws OperationException{
        return bQueryUpdateExecutor.execute(queryJSON);
    }

    public JSONObject update(BQueryUpdateStatement updateStatement) throws OperationException {
        return bQueryUpdateExecutor.execute(updateStatement);
    }

    public JSONObject update(final String datastore, final String collection, final JSONObject rowJSON) throws OperationException{
        BQueryUpdateStatement bQueryUpdateStatement = new BQueryUpdateStatement(datastore, collection,
                SchemaStore.getInstance().getSchema(datastore, collection).getPrimary(),
                rowJSON);
        return update(bQueryUpdateStatement);
    }


    public JSONObject remove(JSONObject queryJSON) throws OperationException{
        return bQueryRemoveExecutor.execute(queryJSON);
    }

    public JSONObject remove(BQueryRemoveStatement removeStatement) throws OperationException{
        return bQueryRemoveExecutor.execute(removeStatement);
    }

}
