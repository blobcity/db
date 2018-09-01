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
 * Parses a message to the corresponding type implementation
 *
 * @author sanketsarang
 */
public class MessageParser {

    public static Message parse(final String messageString) throws OperationException {
        Message message = null;
        JSONObject jsonObject = new JSONObject(messageString);
        MessageTypes messageTypes = MessageTypes.fromType(jsonObject.getString("mtyp"));
        switch(messageTypes) {
            case JSON_QUERY:
                message = new JsonQueryMessage();
                break;
            case NODE_CONNECTION_HEADER:
                message = new NodeConnectionHeaderMessage();
                break;
            case SQL_QUERY:
                message = new SqlMessage();
                break;
        }
        
        if(message == null) {
            return null;
        }
        
        message.init(jsonObject);
        return message;
    }
}
