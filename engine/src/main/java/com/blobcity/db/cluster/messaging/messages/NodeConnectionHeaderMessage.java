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
 * This object represents the first header message that is passed when a node opens a connection with any other node.
 * The message allows the destination node to identify the node that is opening the connection.
 *
 * @author sanketsarang
 */
public class NodeConnectionHeaderMessage extends AbstractMessage implements Message {

    private final MessageTypes messageType = MessageTypes.NODE_CONNECTION_HEADER;

    private String nodeId;
    private String ip;

    public NodeConnectionHeaderMessage() {
        this.nodeId = null;
        this.ip = null;
    }

    public NodeConnectionHeaderMessage(final String nodeId) {
        this.nodeId = nodeId;
        this.ip = null;
    }
    
    public NodeConnectionHeaderMessage(final String nodeId, final String ip) {
        this.nodeId = nodeId;
        this.ip = ip;
    }

    @Override
    public void init(JSONObject jsonObject) throws OperationException {
        superInit(jsonObject);
        JSONObject payloadJson = jsonObject.getJSONObject("p");
        if (payloadJson.has("node-id")) {
            this.nodeId = jsonObject.getString("node-id");
        }
        if (payloadJson.has("ip")) {
            this.ip = jsonObject.getString("ip");
        }
    }

    @Override
    public JSONObject toJson() {
        JSONObject jsonObject = superToJson();
        JSONObject payloadJson = new JSONObject();
        if (nodeId != null) {
            payloadJson.put("node-id", nodeId);
        }
        if (ip != null) {
            payloadJson.put("ip", ip);
        }
        jsonObject.put("p", payloadJson);
        return jsonObject;
    }

    /* Getters and Setters */
    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }
    
    @Override
    public MessageTypes getMessageType() {
        return messageType;
    }
}
