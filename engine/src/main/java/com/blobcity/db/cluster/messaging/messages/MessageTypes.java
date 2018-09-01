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

/**
 * Enumeration storing the various types of messages that are possible for transmission over inter-node communication
 * channels
 *
 * @author sanketsarang
 */
public enum MessageTypes {

    JSON_QUERY("json-query"),
    SQL_QUERY("sql-query"),
    JSON_RESULT_SET("json-result-set"),
    NODE_CONNECTION_HEADER("node-connection-header");

    private String type;

    private MessageTypes(final String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }

    /**
     * Gets the {@link MessageTypes} corresponding to the string type code. The type code string is case-sensitive
     *
     * @param typeString the string type code of a valid type
     * @return an instance of {@link MessageTypes} mapping to the type code string if a valid type code string is
     * passed; <code>null</code> otherwise
     */
    public static MessageTypes fromType(final String typeString) {
        for (MessageTypes messageType : MessageTypes.values()) {
            if (typeString.equals(messageType.getType())) {
                return messageType;
            }
        }

        return null;
    }
}
