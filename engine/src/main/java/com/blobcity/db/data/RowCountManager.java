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

package com.blobcity.db.data;

import com.blobcity.db.exceptions.ErrorCode;
import com.blobcity.db.exceptions.OperationException;
import com.blobcity.db.sql.util.PathUtil;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Reading and writing row count values to and from the table specific row count files
 *
 * @author sanketsarang
 */
@Component
public class RowCountManager {

    /**
     * Reads the table row count file located at BC_HOME/{app}/db/{table}/meta/row-count.bdb
     *
     * @param app the application id of the BlobCity application
     * @param table name of table within the application
     * @return the number of rows currently in the specified table as reported by the row count file
     * @throws OperationException if an I/O error occurs in reading the file. Also thrown if the application id and table combinations are invalid/inexistent
     */
    public long readCount(final String app, final String table) throws OperationException {
        Path path = FileSystems.getDefault().getPath(PathUtil.tableRowCountFilePath(app, table));
        try {
            return Long.parseLong(Files.readAllLines(path, Charset.defaultCharset()).get(0));
        } catch (IOException | NumberFormatException ex) {
            LoggerFactory.getLogger(RowCountManager.class.getName()).error(null, ex);
            throw new OperationException(ErrorCode.COLLECTION_ROW_COUNT_ERROR);
        }
    }

    /**
     * Writes the table row count to the file located at BC_HOME/{app}/db/{table}/meta/row-count.bdb. The function will either create or update the file
     *
     * @param app the application id of the BlobCity application
     * @param table name of table within the application
     * @param count the row count value to be stored in the file
     * @throws OperationException if an I/O error occurs while writing to the file. Also if the app and table combination is invalid/inexistent
     */
    public void writeCount(final String app, final String table, final long count) throws OperationException {
        Path path = FileSystems.getDefault().getPath(PathUtil.tableRowCountFilePath(app, table));
        try {
            Files.write(path, ("" + count).getBytes());
        } catch (IOException ex) {
            LoggerFactory.getLogger(RowCountManager.class.getName()).error(null, ex);
            throw new OperationException(ErrorCode.COLLECTION_ROW_COUNT_ERROR);
        }
    }
}
