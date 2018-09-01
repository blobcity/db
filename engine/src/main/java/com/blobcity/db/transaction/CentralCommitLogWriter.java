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

import com.blobcity.db.constants.BSql;
import com.blobcity.db.locks.LockType;
import com.blobcity.lib.query.Query;
import org.springframework.stereotype.Component;

import java.io.*;
import java.util.concurrent.locks.Lock;

/**
 * @author sanketsarang
 */
@Component //singleton
public class CentralCommitLogWriter {
    private BufferedWriter writer;

    public CentralCommitLogWriter() {
        try {
            writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(BSql.CURRENT_COMMIT_LOG_FILE)));
        } catch (FileNotFoundException e) {
            e.printStackTrace();

            //TODO: Figure out how to handle this. Maybe product boot should be failed
        }
    }

    public void write(Query query) {
        try {
            writer.write(System.currentTimeMillis() + "|" + query.toJsonString());
            writer.newLine();
            writer.flush();
        } catch (IOException e) {
            e.printStackTrace();

            //TODO: Figure out how to handle this. Maybe commits should be rolled back if failure happens here
        }
    }
}
