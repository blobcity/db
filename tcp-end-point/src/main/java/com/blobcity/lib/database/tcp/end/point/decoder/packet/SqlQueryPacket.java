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

package com.blobcity.lib.database.tcp.end.point.decoder.packet;

import com.blobcity.lib.database.tcp.end.point.decoder.packet.base.Header;
import com.blobcity.lib.database.tcp.end.point.decoder.packet.base.Packet;

/**
 * Represents a packet containing an SQL query
 *
 * @author javatarz (Karun Japhet)
 */
public class SqlQueryPacket extends Packet {

    private final String data;

    /**
     * Creates an instance of a packet containing an SQL query
     *
     * @param header {@link Header} object representing the header information for this packet
     * @param dataBytes bytes of data remaining in the message after reading the header. Represents a SQL query
     */
    public SqlQueryPacket(Header header, byte[] dataBytes) {
        super(header);
        this.data = new String(dataBytes);
    }

    @Override
    public String getData() {
        return data;
    }

    @Override
    public String toString() {
        return "SqlQueryPacket{header=\"" + header + "\", data=\"" + data + "\"}";
    }
}
