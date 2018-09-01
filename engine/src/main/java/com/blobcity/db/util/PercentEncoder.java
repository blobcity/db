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

package com.blobcity.db.util;

import java.io.ByteArrayOutputStream;
import java.io.UnsupportedEncodingException;
import java.util.BitSet;

/**
 * Performs percent encoding. http://en.wikipedia.org/wiki/Percent-encoding . Reserved characters are defined by a
 * BitSet rather than RFC 3986 (or other RFCs)
 *
 * @author akshaydewan
 */
public class PercentEncoder {

    /**
     * Encodes a byte array into percent encoding
     *
     * @param source The byte-representation of the string.
     * @param bitSet The BitSet for characters to skip encoding
     * @return The percent-encoded byte array
     */
    public static byte[] encode(byte[] source, BitSet bitSet) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream(source.length * 2);
        for (int i = 0; i < source.length; i++) {
            int b = source[i];
            if (b < 0) {
                b += 256;
            }
            if (bitSet.get(b)) {
                bos.write(b);
            } else {
                bos.write('%');
                char hex1 = Character.toUpperCase(Character.forDigit((b >> 4) & 0xF, 16));
                char hex2 = Character.toUpperCase(Character.forDigit(b & 0xF, 16));
                bos.write(hex1);
                bos.write(hex2);
            }
        }
        return bos.toByteArray();
    }

    /**
     * Decodes a percent-encoded string as per the specified encoding
     *
     * @param source A percent-encoded string
     * @param encoding The character encoding of the string
     * @return The decoded string
     * @throws UnsupportedEncodingException If the encoding is unknown.
     */
    public static String decode(String source, String encoding) throws UnsupportedEncodingException {
        int length = source.length();
        ByteArrayOutputStream bos = new ByteArrayOutputStream(length);
        for (int i = 0; i < length; i++) {
            int ch = source.charAt(i);
            if (ch == '%') {
                if ((i + 2) < length) {
                    char hex1 = source.charAt(i + 1);
                    char hex2 = source.charAt(i + 2);
                    int u = Character.digit(hex1, 16);
                    int l = Character.digit(hex2, 16);
                    bos.write((char) ((u << 4) + l));
                    i += 2;
                } else {
                    throw new IllegalArgumentException("Invalid encoded sequence \"" + source.substring(i) + "\"");
                }
            } else {
                bos.write(ch);
            }
        }
        return new String(bos.toByteArray(), encoding);
    }

}
