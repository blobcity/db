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
import com.blobcity.lib.database.tcp.end.point.decoder.util.ByteUtil;
import java.util.Arrays;

/**
 * Represents a Login Packet sent by the client towards the server
 *
 * @author javatarz (Karun Japhet)
 */
public class LoginRequestPacket extends Packet {

    private final String userName;
    private final String passwordHash;
    private final String dbName;

    /**
     * Creates a login packet from bytes of data sent by the client
     *
     * @param header {@link Header} object representing the header information for this packet
     * @param dataBytes bytes of data remaining in the message after reading the header
     */
    public LoginRequestPacket(final Header header, final byte[] dataBytes) {
        super(header);
        final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(LoginRequestPacket.class);
        logger.debug("Unwrapping LoginRequestPacket with data {}", dataBytes);

        int index = 0;
        final int userNameLen = ByteUtil.getInt(Arrays.copyOfRange(dataBytes, 0, 2));
        index += 2;
        logger.trace("User Name len = {} | Index = {}", userNameLen, index);
        this.userName = new String(Arrays.copyOfRange(dataBytes, index, index + userNameLen));
        index += userNameLen;
        logger.trace("User Name = {} | Index = {}", userName, index);

        final int passwordHashLen = ByteUtil.getInt(Arrays.copyOfRange(dataBytes, index, index + 2));
        index += 2;
        logger.trace("Password Hash len = {} | Index = {}", passwordHashLen, index);
        this.passwordHash = new String(Arrays.copyOfRange(dataBytes, index, index + passwordHashLen));
        index += passwordHashLen;
        logger.trace("Password Hash = {} | Index = {}", userName, index);

        final int dbNameLen = ByteUtil.getInt(Arrays.copyOfRange(dataBytes, index, index + 1));
        index++;
        logger.trace("DB Name len = {} | Index = {}", dbNameLen, index);
        this.dbName = new String(Arrays.copyOfRange(dataBytes, index, index + dbNameLen));
        logger.trace("DB Name = {} | Index = {}", dbName, index);
    }

    @Override
    public String toString() {
        return "LoginRequestPacket{header=\"" + header
                + "\", userName=\"" + userName
                + "\", passwordHash=\"" + passwordHash
                + "\", dbName=\"" + dbName + "\"}";
    }

    @Override
    public String getData() {
        return userName + "|" + passwordHash + "|" + dbName;
    }

    public String getUserName() {
        return userName;
    }

    public String getPasswordHash() {
        return passwordHash;
    }

    public String getDbName() {
        return dbName;
    }
}
