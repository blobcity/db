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
 * Represents a JSON based query message being passed over the cluster
 *
 * @author sanketsarang
 */
public class JsonQueryMessage extends AbstractMessage implements Message {

    private final MessageTypes messageType = MessageTypes.JSON_QUERY;
    private JSONObject queryJson;
    
    public JsonQueryMessage() {
        super.request = true;
    }

    @Override
    public void init(JSONObject jsonObject) throws OperationException {
        superInit(jsonObject);
        JSONObject payloadJson = jsonObject.getJSONObject("p");
        this.queryJson = payloadJson.getJSONObject("q");
    }

    @Override
    public JSONObject toJson() {
        JSONObject jsonObject = superToJson();
        JSONObject payloadJson = new JSONObject();
        payloadJson.put("q", queryJson);
        jsonObject.put("p", payloadJson);
        return jsonObject;
    }
    
    @Override
    public MessageTypes getMessageType() {
        return messageType;
    }

    public JSONObject getQueryJson() {
        return queryJson;
    }

    public void setQueryJson(JSONObject queryJson) {
        this.queryJson = queryJson;
    }
}
