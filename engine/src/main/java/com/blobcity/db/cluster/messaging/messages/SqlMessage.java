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

package com.blobcity.db.cluster.messaging.messages;

import com.blobcity.db.exceptions.OperationException;
import org.json.JSONObject;

/**
 * Represents a JSON object containing an SQL command. Used for internode communication
 *
 * @author sanketsarang
 */
public class SqlMessage extends AbstractMessage implements Message {

    private final MessageTypes messageType = MessageTypes.SQL_QUERY;
    private String sql;

    public SqlMessage() {
        //do nothing
    }

    public SqlMessage(final String sql) {
        this.sql = sql;
    }

    @Override
    public void init(JSONObject jsonObject) throws OperationException {
        superInit(jsonObject);
        super.request = true;
        JSONObject payloadJson = jsonObject.getJSONObject("p");
        this.sql = payloadJson.getString("q");
    }

    @Override
    public JSONObject toJson() {
        JSONObject jsonObject = superToJson();
        JSONObject payloadJson = new JSONObject();
        payloadJson.put("q", this.sql);
        jsonObject.put("p", payloadJson);
        return jsonObject;
    }

    @Override
    public MessageTypes getMessageType() {
        return messageType;
    }
    
    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }
}
