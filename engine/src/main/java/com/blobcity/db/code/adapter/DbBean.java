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

package com.blobcity.db.code.adapter;

import com.blobcity.db.sp.adapter.CollectionType;
import com.blobcity.db.sp.adapter.Db;
import com.blobcity.db.sp.adapter.StoredProcedureException;
import org.json.JSONObject;

import java.util.List;

/**
 * @author sanketsarang
 */
public class DbBean implements Db {

    @Override
    public JSONObject sql(String sql) throws StoredProcedureException {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("ack", "1");

        return jsonObject;
    }

    @Override
    public JSONObject insertJson(String s, String s1, List<JSONObject> list) throws StoredProcedureException {
        return null;
    }

    @Override
    public JSONObject insertXml(String s, String s1, List<String> list) throws StoredProcedureException {
        return null;
    }

    @Override
    public JSONObject insertCsv(String s, String s1, List<String> list, List<String> list1) throws StoredProcedureException {
        return null;
    }

    @Override
    public JSONObject insertText(String s, String s1, List<String> list) throws StoredProcedureException {
        return null;
    }

    @Override
    public JSONObject insertText(String s, String s1, List<String> list, String s2) throws StoredProcedureException {
        return null;
    }

    @Override
    public void createDs(String s) throws StoredProcedureException {

    }

    @Override
    public void createCollection(String s, String s1, CollectionType collectionType) throws StoredProcedureException {

    }

    @Override
    public void dropDs(String s) throws StoredProcedureException {

    }

    @Override
    public void dropCollection(String s, String s1) throws StoredProcedureException {

    }

    @Override
    public void tuncateDs(String s) throws StoredProcedureException {

    }

    @Override
    public void truncateCollection(String s, String s1) throws StoredProcedureException {

    }
}
