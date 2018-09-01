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

import com.blobcity.db.exceptions.ErrorCode;
import com.blobcity.db.exceptions.OperationException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.BitSet;
import org.slf4j.LoggerFactory;

/**
 *
 * Encodes and decodes file names for saving on the disk.
 *
 * @author sanketsarang
 * @author akshaydewan
 */
public class FileNameEncoding {

    public static final int MAX_LENGTH = 255;
    public static final String CHARSET = "UTF-8";

    /**
     * The characters which are not encoded
     */
    private static final BitSet NOT_ENCODED;
    /**
     * Used for OS reserved characters of "." and ".." The current implementation is an empty bitset
     */
    private static final BitSet OS_RESERVED;

    static {
        BitSet alpha = new BitSet(256);
        for (int i = 'a'; i <= 'z'; i++) {
            alpha.set(i);
        }
        for (int i = 'A'; i <= 'Z'; i++) {
            alpha.set(i);
        }
        BitSet digit = new BitSet(256);
        for (int i = '0'; i <= '9'; i++) {
            digit.set(i);
        }

        NOT_ENCODED = new BitSet(256);
        NOT_ENCODED.or(alpha);
        NOT_ENCODED.or(digit);
        NOT_ENCODED.set('.');
        NOT_ENCODED.set('-');
        NOT_ENCODED.set('_');

        OS_RESERVED = new BitSet(1);
    }

    private static String encode(String source, String encoding) throws UnsupportedEncodingException {
        byte[] bytes;
        if (source.length() == 1 && source.equals(".")) {
            bytes = PercentEncoder.encode(source.getBytes(encoding), OS_RESERVED);
        } else if (source.length() == 2 && source.equals("..")) {
            bytes = PercentEncoder.encode(source.getBytes(encoding), OS_RESERVED);
        } else {
            bytes = PercentEncoder.encode(source.getBytes(encoding), NOT_ENCODED);
        }
        return new String(bytes, "US-ASCII");
    }

    /**
     * Encodes a string for saving as a file. Percent-encoding is used. The implementation encodes all characters other than those specified by the POSIX Fully
     * Portable Filenames and the '%' character
     *
     * @param fileName The name of the string to the encoded
     * @return The percent-encoded file name
     * @throws OperationException If the encoded string exceeds the maximum limit, or if the encoding fails
     */
    public static final String encode(final String fileName) throws OperationException {
        try {
            String encoded = encode(fileName, CHARSET);
            if (encoded.length() > MAX_LENGTH) {
                throw new OperationException(ErrorCode.STRING_LENGTH_EXCEEDED, "Length exceeded : " + fileName);
            }
            return encoded;
        } catch (UnsupportedEncodingException ex) {
            LoggerFactory.getLogger(FileNameEncoding.class.getName()).error(null, ex);
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "Failed to encode string: " + fileName);
        }
    }

    /**
     * Decodes a percent-encoded string
     *
     * @param fileName The encoded string
     * @return The decoded string
     * @throws OperationException If the decoding operation fails
     */
    public static final String decode(final String fileName) throws OperationException {
        try {
            return URLDecoder.decode(fileName, CHARSET);
        } catch (UnsupportedEncodingException ex) {
            LoggerFactory.getLogger(FileNameEncoding.class.getName()).error(null, ex);
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "Failed to decode string: " + fileName);
        }
    }
}
