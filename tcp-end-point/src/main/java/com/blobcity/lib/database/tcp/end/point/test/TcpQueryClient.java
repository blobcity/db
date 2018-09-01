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

package com.blobcity.lib.database.tcp.end.point.test;

import com.blobcity.lib.database.tcp.end.point.decoder.exception.DecodeException;
import com.blobcity.lib.database.tcp.end.point.decoder.packet.field.MessageType;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.math.BigInteger;
import java.net.Socket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a test client application to connect to the TCP end point as a service until an appropriate adapter is available to test this communication. None of
 * this code under ANY circumstance should be considered the right way to do TCP communication. Methods to wrap data and create packet headers can be reused
 * after review.
 * 
 * YOYO.
 *
 * @author javatarz (Karun Japhet)
 */
public class TcpQueryClient {

    private static final Logger logger = LoggerFactory.getLogger(TcpQueryClient.class);
    private static Socket socket;
    private static PrintWriter out;
    private static InputStreamReader isr;

    public static void main(String[] args) throws IOException, InterruptedException, DecodeException {
        final String loginMessage = login();
        final String queryMessage = query();

        final String hostname = "localhost";
        final int port = 10100;
        connect(hostname, port);

        write(loginMessage);
        write(queryMessage);
        close();
    }

    private static String login() throws IOException {
        final String userName = "karun";
        final String passwordHash = "e80eded141e1295d694cd35cf2b8f675";//"wordpass";
        final String dbName = "2aa1b58a-345d-4de6-bd69-c4280b63e179";

        final String loginMessage = getLoginMessage(userName, passwordHash, dbName);
        final String transmittableMessage = getTransmittableMessage(MessageType.LOGIN_REQ, loginMessage, 0);

        return transmittableMessage;
    }

    private static String query() throws IOException {
        final String baseMessage = "select * from `Login`";
        final String queryMessage = getTransmittableMessage(MessageType.SQL_BATCH, baseMessage, 0);

        return queryMessage;
    }

    private static String getLoginMessage(final String userName, final String passwordHash, final String dbName) {
        byte[] userNameLen = BigInteger.valueOf(userName.length()).toByteArray();
        byte[] passwordHashLen = BigInteger.valueOf(passwordHash.length()).toByteArray();
        byte[] dbNameLen = BigInteger.valueOf(dbName.length()).toByteArray();

        userNameLen = new byte[]{getNormalizedByte(userNameLen, 1), getNormalizedByte(userNameLen, 0)};
        passwordHashLen = new byte[]{getNormalizedByte(passwordHashLen, 1), getNormalizedByte(passwordHashLen, 0)};
        dbNameLen = new byte[]{getNormalizedByte(dbNameLen, 0)};

        return new String(userNameLen) + userName + new String(passwordHashLen) + passwordHash + new String(dbNameLen) + dbName;
    }

    private static String getTransmittableMessage(final MessageType messageType, final String baseMessage, final int threadId) {
        final byte type = messageType.getTypeByte();
        final byte status = 0x1;
        final int lenInt = 8 + baseMessage.length();
        final byte[] length = BigInteger.valueOf(lenInt).toByteArray();
        final byte[] spid = BigInteger.valueOf(threadId).toByteArray();
        final byte packetId = 0x0;
        final byte window = 0x0;
        byte[] headerBytes = {type, status, getNormalizedByte(length, 1), getNormalizedByte(length, 0), getNormalizedByte(spid, 1), getNormalizedByte(spid, 0), packetId, window};

        return new String(headerBytes) + baseMessage;
    }

    private static byte getNormalizedByte(final byte[] byteArr, int position) {
        return position < byteArr.length ? byteArr[position] : 0x0;
    }

    private static void connect(final String hostname, final int port) throws IOException {
        socket = new Socket(hostname, port);
        out = new PrintWriter(socket.getOutputStream(), true);
        isr = isr = new InputStreamReader(socket.getInputStream());
    }

    private static void write(final String message) throws IOException {
        final long pollTime = 500;

        final StringBuilder sb = new StringBuilder();
        boolean isFirst = true;
        long firstCharTime = -1;
        long firstCharTimeDiff = -1;

        logger.info("Request sent");
        out.print(message);
        out.flush();

        long lastReadTime = System.currentTimeMillis();
        long startTime = lastReadTime;
        do {
            if (isr.ready()) {
                final int chr = isr.read();
                sb.append((char) chr);
                lastReadTime = System.currentTimeMillis();
                if (isFirst) {
                    firstCharTime = lastReadTime;
                    firstCharTimeDiff = lastReadTime - startTime;
                }
            }
        } while (System.currentTimeMillis() - lastReadTime < pollTime);

        logger.info("Request: {}\nResponse: {}\nFirst Char Seek Time: {} ({})\nFull data transport time: {}\nStart: {}\nEnd: {}", message, sb.toString(), firstCharTimeDiff, firstCharTime, lastReadTime - startTime, startTime, lastReadTime);
    }

    private static void close() throws IOException {
        closeQueitly(out);
        closeQueitly(isr);
        closeQueitly(socket);
    }

    private static void closeQueitly(final Closeable closeable) {
        try {
            closeable.close();
        } catch (final IOException | NullPointerException e) {
            // do nothing
        }
    }
}
