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

package com.blobcity.util.json;

import java.util.List;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author sanketsarang
 */
public class JsonMessages {

    public static final String ERROR_ACKNOWLEDGEMENT = "{\"ack\":\"0\"}";
    public static final String SUCCESS_ACKNOWLEDGEMENT = "{\"ack\":\"1\"}";

    public static final JSONObject SUCCESS_ACKNOWLEDGEMENT_OBJECT;

    static {
        JSONObject internalJson = null;
        try {
            internalJson = new JSONObject(SUCCESS_ACKNOWLEDGEMENT);
        } catch (JSONException ex) {
            //do nothing
        }

        SUCCESS_ACKNOWLEDGEMENT_OBJECT = internalJson;
    }

    public static JSONObject successWithWarnings(List<String> warnings) {
        return new JSONObject().put("ack", 1).put("warnings", warnings);
    }

    public static JSONObject errorWithCause(final String cause) {
        return new JSONObject().put("ack", 0).put("cause", cause);
    }
    
    public static JSONObject clusterCommandResponseAck(final String requestId, final String nodeId, final String type, final boolean success) {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("rid", requestId);
        jsonObject.put("nid", nodeId);
        jsonObject.put("req", false); //request = false means this is a response message
        jsonObject.put("typ", type);
        jsonObject.put("ack", success ? "1" : "0");
        return jsonObject;
    }
    
    public static JSONObject clusterCommandRequestMessage(final String requestId, final String nodeId, final String type, final JSONObject payloadJson) {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("rid", requestId);
        jsonObject.put("nid", nodeId);
        jsonObject.put("req", true); //request = true means this is a request message
        jsonObject.put("typ", type);
        jsonObject.put("p", payloadJson);
        return jsonObject;
    }
}
