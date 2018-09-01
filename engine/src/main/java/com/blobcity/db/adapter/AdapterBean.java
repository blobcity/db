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

package com.blobcity.db.adapter;

import com.blobcity.db.requests.RequestHandlingBean;
import com.blobcity.db.sp.adapter.CollectionType;
import com.blobcity.db.sp.adapter.Db;
import com.blobcity.db.sp.adapter.StoredProcedureException;
import com.blobcity.lib.query.Query;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * @author sanketsarang
 */
@Component
public class AdapterBean implements Db {

    @Autowired
    private RequestHandlingBean requestHandlingBean;

    @Override
    public JSONObject sql(String sql) throws StoredProcedureException {
        Query query = new Query();
        return null;
    }

    @Override
    public JSONObject insertJson(String ds, String collection, List<JSONObject> records) throws StoredProcedureException {
        return null;
    }

    @Override
    public JSONObject insertXml(String ds, String collection, List<String> records) throws StoredProcedureException {
        return null;
    }

    @Override
    public JSONObject insertCsv(String ds, String collection, List<String> columns, List<String> records) throws StoredProcedureException {
        return null;
    }

    @Override
    public JSONObject insertText(String ds, String collection, List<String> records) throws StoredProcedureException {
        return null;
    }

    @Override
    public JSONObject insertText(String ds, String collection, List<String> records, String interpreter) throws StoredProcedureException {
        return null;
    }

    @Override
    public void createDs(String ds) throws StoredProcedureException {

    }

    @Override
    public void createCollection(String ds, String collection, CollectionType collectionType) throws StoredProcedureException {

    }

    @Override
    public void dropDs(String ds) throws StoredProcedureException {

    }

    @Override
    public void dropCollection(String ds, String collection) throws StoredProcedureException {

    }

    @Override
    public void tuncateDs(String ds) throws StoredProcedureException {

    }

    @Override
    public void truncateCollection(String ds, String collection) throws StoredProcedureException {

    }
}
