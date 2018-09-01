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

package com.blobcity.db.bquery.statements;

import com.blobcity.db.constants.BQueryCommands;
import com.blobcity.db.constants.BQueryParameters;
import com.blobcity.util.json.Jsonable;
import org.json.JSONObject;

/**
 *
 * @author sanketsarang
 */
public class BQueryInsertStatement implements Jsonable {
    
    private String app;
    private String table;
    private String pk;
    private JSONObject data;
    
    public BQueryInsertStatement() {
        //do nothing
    }
    
    public BQueryInsertStatement(final String app, final String table, final JSONObject data) {
        this.app = app;
        this.table = table;
        this.data = data;
        this.pk = null;
    }
    
    public BQueryInsertStatement(final String app, final String table, final String pk, final JSONObject data) {
        this.app = app;
        this.table = table;
        this.pk = pk;
        this.data = data;
    }

    public String getApp() {
        return app;
    }

    public void setApp(String app) {
        this.app = app;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getPk() {
        return pk;
    }

    public void setPk(String pk) {
        this.pk = pk;
    }

    public JSONObject getData() {
        return data;
    }

    public void setData(JSONObject data) {
        this.data = data;
    }
    
    @Override
    public JSONObject toJson() {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put(BQueryParameters.ACCOUNT, app);
        jsonObject.put(BQueryParameters.TABLE, table);
        jsonObject.put(BQueryParameters.QUERY, BQueryCommands.INSERT.getCommand());
        jsonObject.put(BQueryParameters.PAYLOAD, data);
        return jsonObject;
    }

    public static BQueryInsertStatement fromJson(JSONObject jsonObject) {
        BQueryInsertStatement bQueryInsertStatement = new BQueryInsertStatement();
        bQueryInsertStatement.setApp(jsonObject.getString(BQueryParameters.ACCOUNT));
        bQueryInsertStatement.setTable(jsonObject.getString(BQueryParameters.TABLE));
        bQueryInsertStatement.setData(jsonObject.getJSONObject(BQueryParameters.PAYLOAD));
        return bQueryInsertStatement;
    }
    
    public static BQueryInsertStatement fromJson(JSONObject jsonObject, String primaryKeyColumnName) {
        BQueryInsertStatement bQueryInsertStatement = new BQueryInsertStatement();
        bQueryInsertStatement.setApp(jsonObject.getString(BQueryParameters.ACCOUNT));
        bQueryInsertStatement.setTable(jsonObject.getString(BQueryParameters.TABLE));
        bQueryInsertStatement.setData(jsonObject.getJSONObject(BQueryParameters.PAYLOAD));
        bQueryInsertStatement.setPk(bQueryInsertStatement.getData().getString(primaryKeyColumnName));
        return bQueryInsertStatement;
    }
}
