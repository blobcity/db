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

/**
 * Base type for all TCP packets
 *
 * @author javatarz (Karun Japhet)
 */
public abstract class Packet {

    protected final Header header;

    /**
     * Constructs an instance of {@link Packet}
     *
     * @param header {@link Header} instance associated with this packet
     */
    protected Packet(final Header header) {
        this.header = header;
    }

    /**
     * @return instance of {@link Header} that caused this packet to be created
     */
    public Header getHeader() {
        return header;
    }

    /**
     * @return data held by each packet. For packets with singular data fields, this method returns the required data. For packets with multiple data fields,
     * this method appends all data in a specific format (usually separated by a separator like '|') and provides extra getters to individual fields.
     */
    public abstract String getData();
}
