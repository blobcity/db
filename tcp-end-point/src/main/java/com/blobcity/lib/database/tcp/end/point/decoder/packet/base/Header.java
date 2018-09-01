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

package com.blobcity.lib.database.tcp.end.point.decoder.packet.base;

import com.blobcity.lib.database.tcp.end.point.decoder.exception.DecodeException;
import com.blobcity.lib.database.tcp.end.point.decoder.packet.LoginRequestPacket;
import com.blobcity.lib.database.tcp.end.point.decoder.packet.SqlQueryPacket;
import com.blobcity.lib.database.tcp.end.point.decoder.packet.field.MessageStatus;
import com.blobcity.lib.database.tcp.end.point.decoder.packet.field.MessageType;

/**
 * Base type for all TCP headers
 *
 * @author javatarz (Karun Japhet)
 */
public abstract class Header {

    /**
     * Creates a packet from current data based on the current {@link MessageType} specified by {@link #getType()}
     *
     * @param dataBytes bytes of data in the message after removing the header bytes
     * @return a specific instance of {@link Packet} based on {@link #getType()}
     * @throws DecodeException if the packet type specified in the header doesn't have an equivalent create spec defined
     */
    public Packet createPacket(final byte[] dataBytes) throws DecodeException {
        switch (getType()) {
            case SQL_BATCH:
                return new SqlQueryPacket(this, dataBytes);
            case LOGIN_REQ:
                return new LoginRequestPacket(this, dataBytes);
            default:
                throw new DecodeException("Invalid Packet Type specified. Unable to create a packet from header.");
        }
    }

    /**
     * @return Header information as a byte array
     */
    public abstract byte[] asByteArray();

    /**
     * @return {@link MessageType} represented by the header
     */
    public abstract MessageType getType();

    /**
     * @return {@link MessageStatus} represented by the header
     */
    public abstract MessageStatus getMessageStatus();

    /**
     * @return length (in bytes) of the message associated with this header
     */
    public abstract int getLength();

    /**
     * @return process ID on the server corresponding to the current connection
     */
    public abstract int getSpid();

    /**
     * @return packet number of the current packet
     */
    public abstract int getPacketId();

    /**
     * @return window id. Currently not used and is to be ignored.
     */
    public abstract int getWindow();
}
