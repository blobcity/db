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

package com.blobcity.db.watchservice;

import com.blobcity.db.code.CodeExecutor;
import com.blobcity.db.exceptions.OperationException;
import com.blobcity.db.memory.records.JsonRecord;
import com.blobcity.db.requests.RequestHandlingBean;
import com.blobcity.lib.data.Record;
import com.blobcity.lib.query.Query;
import com.blobcity.lib.query.RecordType;
import org.apache.commons.io.input.TailerListenerAdapter;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import java.util.Arrays;

/**
 * A listener for file tailing operations which inserts the the added row to database
 *
 * @author sanketsarang
 */
@Component
public class FileTailListener extends TailerListenerAdapter {

    private static final Logger logger = LoggerFactory.getLogger(FileTailListener.class);

    private String datastore;
    private String collection;
    private String filePath;
    private String interpreter;

    @Autowired @Lazy
    private CodeExecutor codeExecutor;
    @Autowired @Lazy
    private RequestHandlingBean requestHandlingBean;


    public void setParams(final String datastore, final String collection, final String interpreter, final String filePath){
        this.datastore = datastore;
        this.collection = collection;
        this.interpreter = interpreter;
        this.filePath = filePath;
    }

    @Override
    public void handle(String line){
        JSONObject rowJson;

        if(line == null || line.isEmpty()) {
            return;
        }

        if(interpreter == null) {
            rowJson = new JSONObject();
            rowJson.put("_txt", line);
        } else {
            try {
                rowJson = codeExecutor.executeDataInterpreter(datastore, interpreter, line);
            } catch (OperationException ex) {
                logger.error("error in running " + interpreter + " for " + datastore + "." + collection + " watch service for line: " + line);
                rowJson = new JSONObject();
            }
        }

        logger.debug(filePath + ","+datastore+","+collection+":"+rowJson.toString());
        Query query = new Query().insertQuery(datastore, collection, Arrays.asList(new Record[]{new JsonRecord(rowJson)}), RecordType.JSON);
        requestHandlingBean.newRequest(query);
    }
}
