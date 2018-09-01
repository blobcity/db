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
import com.blobcity.lib.database.tcp.end.point.decoder.exception.DecodeException;
import com.blobcity.lib.database.tcp.end.point.decoder.packet.field.MessageStatus;
import com.blobcity.lib.database.tcp.end.point.decoder.packet.field.MessageType;
import java.math.BigInteger;
import java.util.Arrays;

/**
 * Represents the information specified by a TCP header
 *
 * @author javatarz (Karun Japhet)
 */
public class PacketHeader extends Header {

    public static final int HEADER_SIZE = 8;

    private final byte typeByte;
    private final byte statusByte;
    private final byte[] lengthByteArray;
    private final byte[] spidByteArray;
    private final byte packetIdByte;
    private final byte windowByte;

    private final MessageType type;
    private final MessageStatus status;
    private final int length;
    private final int spid;
    private final int packetId;
    private final int window;

    /**
     * Creates an instance of a packet header based on the bytes of information specified by the header
     *
     * @param headerBytes bytes of header represented by the header
     * @throws DecodeException if decoding the header isn't possible due to bad data
     */
    public PacketHeader(final byte[] headerBytes) throws DecodeException {
        if (headerBytes.length != HEADER_SIZE) {
            throw new RuntimeException("Invalid header size of " + headerBytes.length);
        }

        this.typeByte = headerBytes[0];
        this.statusByte = headerBytes[1];
        this.lengthByteArray = new byte[]{headerBytes[2], headerBytes[3]};
        this.spidByteArray = new byte[]{headerBytes[4], headerBytes[5]};
        this.packetIdByte = headerBytes[6];
        this.windowByte = headerBytes[7];

        this.type = MessageType.valueOf(typeByte);
        this.status = MessageStatus.valueOf(statusByte);
        this.length = new BigInteger(lengthByteArray).intValueExact();
        this.spid = new BigInteger(spidByteArray).intValueExact();
        this.packetId = new BigInteger(new byte[]{packetIdByte}).intValueExact();
        this.window = new BigInteger(new byte[]{windowByte}).intValueExact();
    }

    @Override
    public byte[] asByteArray() {
        return new byte[]{typeByte, statusByte, lengthByteArray[0], lengthByteArray[1], spidByteArray[0], spidByteArray[1], packetIdByte, windowByte};
    }

    @Override
    public String toString() {
        return "PacketHeader{" + Arrays.toString(asByteArray()) + "}";
    }

    @Override
    public MessageType getType() {
        return type;
    }

    @Override
    public MessageStatus getMessageStatus() {
        return status;
    }

    @Override
    public int getLength() {
        return length;
    }

    @Override
    public int getSpid() {
        return spid;
    }

    @Override
    public int getPacketId() {
        return packetId;
    }

    @Override
    public int getWindow() {
        return window;
    }

    /**
     * @return {@code byte} of data associated with the field {@code type}
     */
    public byte getTypeByte() {
        return typeByte;
    }

    /**
     * @return {@code byte} of data associated with the field {@code status}
     */
    public byte getStatusByte() {
        return statusByte;
    }

    /**
     * @return 2 {@code byte}s of data associated with the field {@code length} as an array
     */
    public byte[] getLengthByteArray() {
        return lengthByteArray;
    }

    /**
     * @return 2 {@code byte}s of data associated with the field {@code spid} as an array
     */
    public byte[] getSpidByteArray() {
        return spidByteArray;
    }

    /**
     * @return {@code byte} of data associated with the field {@code packetId}
     */
    public byte getPacketIdByte() {
        return packetIdByte;
    }

    /**
     * @return {@code byte} of data associated with the field {@code window}
     */
    public byte getWindowByte() {
        return windowByte;
    }
}
