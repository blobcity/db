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

import com.blobcity.db.exceptions.ErrorCode;
import com.blobcity.db.exceptions.OperationException;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Partially implements {@link Message}. Contains default functionality that is common for all implementations of
 * {@link Message}
 *
 * @author sanketsarang
 */
public abstract class AbstractMessage implements Message {

    private static final Logger logger = LoggerFactory.getLogger(AbstractMessage.class.getName());

    protected String requestId;
    protected boolean request;
    protected String senderNodeId;
    protected String targetNodeId;
    protected String masterNodeId;

    protected void superInit(JSONObject jsonObject) throws OperationException {
        MessageTypes messageType = MessageTypes.fromType(jsonObject.getString("mtyp"));
        if (messageType != getMessageType()) {
            logger.error("Internal cluster query error. Message of type " + messageType.getType()
                    + " passed to parser of type " + getMessageType().getType());
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "Message of incorrect type passed to "
                    + "internal cluster message parser");
        }
        this.requestId = jsonObject.getString("rid");
        this.request = jsonObject.getBoolean("req");
        this.masterNodeId = jsonObject.getString("mnid");
        this.senderNodeId = jsonObject.getString("snid");
        if (jsonObject.has("tnid")) {
            this.targetNodeId = jsonObject.getString("tnid");
        } else {
            this.targetNodeId = null;
        }
    }

    protected JSONObject superToJson() {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("mtyp", getMessageType().getType());
        jsonObject.put("rid", getRequestId());
        jsonObject.put("req", isRequest());
        jsonObject.put("mnid", getMasterNodeId());
        jsonObject.put("snid", getSenderNodeId());
        if (getTargetNodeId() != null) {
            jsonObject.put("tnid", getTargetNodeId());
        }
        return jsonObject;
    }

    @Override
    public void init(String jsonString) throws OperationException {
        try {
            init(new JSONObject(jsonString));
        } catch (JSONException ex) {
            throw new OperationException(ErrorCode.CLUSTER_MESSAGE_FORMAT_ERROR);
        }
    }

    @Override
    public void init(JSONObject jsonObject) throws OperationException {
        superInit(jsonObject);
    }

    @Override
    public String getRequestId() {
        return requestId;
    }

    public void setRequestId(String requestId) {
        this.requestId = requestId;
    }

    @Override
    public boolean isRequest() {
        return request;
    }

    public void setRequest(boolean request) {
        this.request = request;
    }

    @Override
    public String getSenderNodeId() {
        return senderNodeId;
    }

    public void setSenderNodeId(String senderNodeId) {
        this.senderNodeId = senderNodeId;
    }

    @Override
    public String getTargetNodeId() {
        return targetNodeId;
    }

    public void setTargetNodeId(String targetNodeId) {
        this.targetNodeId = targetNodeId;
    }

    @Override
    public String getMasterNodeId() {
        return masterNodeId;
    }

    public void setMasterNodeId(String masterNodeId) {
        this.masterNodeId = masterNodeId;
    }

    @Override
    public String toString() {
        return toJson().toString();
    }
}
