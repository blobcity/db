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

package com.blobcity.lib.database.tcp.end.point.decoder.packet.field;

import com.blobcity.lib.database.tcp.end.point.decoder.exception.DecodeException;

/**
 * Convenience class to map all message types to their respective byte codes
 *
 * @author javatarz (Karun Japhet)
 */
public enum MessageType {

    SQL_BATCH((byte) 0x01), TABULAR_RESULT((byte) 0x02), BULK_LOAD_DATA((byte) 0x03), ACK_RESP((byte) 0x10), PRE_LOGIN_REQ((byte) 0x11), LOGIN_REQ((byte) 0x12);

    private final byte typeByte;

    /**
     * Instantiates an instance of {@link MessageType}
     *
     * @param typeByte {@code byte} field representing the type in the message header
     */
    private MessageType(final byte typeByte) {
        this.typeByte = typeByte;
    }

    /**
     * @return {@code byte} field representing the type in the message header
     */
    public byte getTypeByte() {
        return typeByte;
    }

    /**
     * Provides {@link MessageType} instances mapping to specific byte fields
     *
     * @param typeByte {@code byte} field representing the type in the message header
     * @return an instance of {@link MessageType} mapping to the specific byte field
     * @throws DecodeException if no {@link MessageType} instance maps to the specified byte field
     */
    public static MessageType valueOf(final byte typeByte) throws DecodeException {
        switch (typeByte) {
            case 0x01:
                return SQL_BATCH;
            case 0x02:
                return TABULAR_RESULT;
            case 0x03:
                return BULK_LOAD_DATA;
            case 0x10:
                return ACK_RESP;
            case 0x11:
                return PRE_LOGIN_REQ;
            case 0x12:
                return LOGIN_REQ;
            default:
                throw new DecodeException("Invalid type byte with value " + typeByte);
        }
    }
}
