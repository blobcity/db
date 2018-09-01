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
 * Convenience class to map all message statuses to their respective byte codes
 *
 * @author javatarz (Karun Japhet)
 */
public enum MessageStatus {

    NORMAL, END_OF_MESSAGE;

    /**
     * Provides {@link MessageStatus} instances mapping to specific byte fields
     *
     * @param statusByte {@code byte} field representing the status in the message header
     * @return an instance of {@link MessageStatus} mapping to the specific byte field
     * @throws DecodeException if no {@link MessageStatus} instance maps to the specified byte field
     */
    public static MessageStatus valueOf(final byte statusByte) throws DecodeException {
        switch (statusByte) {
            case 0x00:
                return NORMAL;
            case 0x01:
                return END_OF_MESSAGE;
            default:
                throw new DecodeException("Invalid status byte with value " + statusByte);
        }
    }
}
