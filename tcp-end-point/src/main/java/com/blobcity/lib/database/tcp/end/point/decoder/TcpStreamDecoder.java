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

package com.blobcity.lib.database.tcp.end.point.decoder;

import com.blobcity.lib.database.tcp.end.point.decoder.exception.DecodeException;
import com.blobcity.lib.database.tcp.end.point.decoder.packet.base.Header;
import com.blobcity.lib.database.tcp.end.point.decoder.packet.base.Packet;
import com.blobcity.lib.database.tcp.end.point.decoder.packet.PacketHeader;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Decodes the incoming stream of bytes into appropriate parsed objects that the TCP handler can consume
 *
 * @author javatarz (Karun Japhet)
 */
public class TcpStreamDecoder extends ByteToMessageDecoder {

    private int headerSize;
    private static final Logger logger = LoggerFactory.getLogger(TcpStreamDecoder.class.getName());

    public TcpStreamDecoder() {
        this.headerSize = PacketHeader.HEADER_SIZE;
    }

    @Override
    protected void decode(final ChannelHandlerContext ctx, final ByteBuf in, final List<Object> out) {
        final long startTime = System.currentTimeMillis();
        try {
            final int readableByteCount = in.readableBytes();
            if (readableByteCount < headerSize) {
                logger.warn("Readable Byte Count ({}) < Header Size ({})", readableByteCount, headerSize);
                return;
            }

            final ByteBuf dataBufBytes = in.readBytes(readableByteCount);
            final byte[] headerBytes = new byte[headerSize];
            dataBufBytes.getBytes(0, headerBytes);
            final Header header = new PacketHeader(headerBytes); // Internally validates the header

            final byte[] dataBytes = new byte[header.getLength() - headerSize];
            dataBufBytes.getBytes(headerSize, dataBytes);
            final Packet packet = header.createPacket(dataBytes);

            logger.info("Packet Type: {} | Request time: {}", packet != null ? packet.getClass() : "null", startTime);

            out.add(packet);
        } catch (DecodeException ex) {
            out.add(ex);
        }
    }

    public int getHeaderSize() {
        return headerSize;
    }

    public void setHeaderSize(int packetSize) {
        this.headerSize = packetSize;
    }
}
