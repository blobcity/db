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

package com.blobcity.db.transaction;

import com.blobcity.db.bsql.BSqlCollectionManager;
import com.blobcity.db.constants.BSql;
import com.blobcity.db.exceptions.ErrorCode;
import com.blobcity.db.exceptions.OperationException;
import com.blobcity.lib.query.Query;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Holds writers to commit log files for each collection in the database
 * @author sanketsarang
 */
@Component //singleton
public class CollectionCommitLogWriter {
    private Map<String, BufferedWriter> writerMap = new HashMap<>();

    public void write(final String ds, final String collection, final Query query) throws OperationException {
        BufferedWriter writer = getWriter(ds, collection);
        synchronized (writer) {
            try {
                writer.write(System.currentTimeMillis() + "|" + query.toJsonString());
                writer.newLine();
                writer.flush();
            } catch (IOException e) {
                throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "Failed to write to commit logs of collection: " + ds + "." + collection);
            }
        }
    }

    private synchronized BufferedWriter getWriter(final String ds, final String collection) throws OperationException {
        final String key = ds + "." + collection;
        if(!writerMap.containsKey(key)) {

            //{data_folder}/{ds}/db/{collection}/commit-logs/commit.log
            final String filePath = BSql.BSQL_BASE_FOLDER + ds + BSql.SEPERATOR + BSql.DATABASE_FOLDER_NAME
                    + BSql.SEPERATOR + collection + BSql.SEPERATOR + BSql.COMMIT_LOGS_FOLDER_NAME
                    + BSql.CURRENT_COMMIT_LOG_FILENAME;

            try {
                writerMap.put(key, new BufferedWriter(new OutputStreamWriter(new FileOutputStream(filePath))));
            } catch(IOException ex) {
                throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "Cannot open commit log writer to path: " + filePath);
            }
        }

        return writerMap.get(key);
    }
}
