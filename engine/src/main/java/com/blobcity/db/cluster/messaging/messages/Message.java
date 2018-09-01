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
 *
 * @author sanketsarang
 */
public interface Message {

    /**
     * Gets the type of the message. Used by the decoder to appropriately decode
     *
     * @return the type of the message
     */
    public MessageTypes getMessageType();

    /**
     * Sets the properties of the respective command from a json string
     *
     * @param jsonString the json string containing properties for object initialization
     * @throws OperationException with {@link ErrorCode} <code>CLUSTER_MESSAGE_FORMAT_ERROR</code> if the message could
     * not be parsed correctly
     */
    public void init(String jsonString) throws OperationException;

    /**
     * Sets the properties of the respective command from a {@link JSONObject}
     *
     * @param jsonObject the json object containing properties for object initialization
     * @throws OperationException with {@link ErrorCode} <code>CLUSTER_MESSAGE_FORMAT_ERROR</code> if the message could
     * not be parsed correctly
     */
    public void init(JSONObject jsonObject) throws OperationException;

    /**
     * Converts the object to a JSON equivalent for transport over the network
     *
     * @return {@link JSONObject} representing the message object properties
     */
    public JSONObject toJson();

    /**
     * Gets the request id of the request to which this message maps to
     *
     * @return the request-id to which the message belongs.
     */
    public String getRequestId();

    /**
     * Gets the nodeId of the node who sent the message
     *
     * @return
     */
    public String getSenderNodeId();

    /**
     * Getter to check if the message is a request or response message
     *
     * @return <code>true</code> if the message is a request message; <code>false</code> for a response message
     */
    public boolean isRequest();

    /**
     * Gets the nodeId of the node to who the message is intended. This is an option parameter
     *
     * @return the node-id of the node to which the message is intended. A value of <code>null</code> indicates the
     * message was intended for broadcast to all nodes in the cluster.
     */
    public String getTargetNodeId();

    /**
     * Gets the node-id of the master that is responsible for executing the request and accumulating responses of
     * individual nodes
     *
     * @return the node-id of the master
     */
    public String getMasterNodeId();
}
