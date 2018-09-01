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

import org.apache.commons.io.input.Tailer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import java.io.File;

/**
 * This bean is responsible for watching files in order to tail them
 *
 * @author sanketsarang
 */
@Component
public class FileWatchService {

    private static final Logger logger = LoggerFactory.getLogger(FileWatchService.class);

    /* Full path to file */
    private String filePath;
    private Tailer tailer;

    private long lastModifiedTime;
    /* Whether to start from the end or beginning of file*/
    private Boolean startFromEnd;
    /* Default time to tail the file after in millis */
    private long defaultTime;
    /* Where to insert the line in the database */
    private String datastore;
    private String collection;
    private String interpreter;

    @Autowired
    private FileTailListener tailListener;


    public FileWatchService(){
        this.defaultTime = 1000;
        this.lastModifiedTime  = -1;
        this.startFromEnd = false;
    }

    /**
     * Set options for reading the file and inserting into database
     *
     * @param filePath: full path to file
     * @param startFromEnd: where to start reading file from end or beginning (default is false)
     * @param datastore: dsSet name where data is to be inserted
     * @param collection: collection name where data is to be inserted
     * @param startFromEnd true if the file should be read from the end; false for reading from beginning
     * @param interpreter name of interpreter to pass each line through; null if no interpreter is to be used
     */
    public void setOptions(final String filePath, final String datastore, final String collection, final boolean startFromEnd, final String interpreter){
        this.filePath = filePath;
        this.datastore = datastore;
        this.collection = collection;
        this.startFromEnd = startFromEnd;
        this.interpreter = interpreter;
    }

    /**
     * start the file watch service
     */
    public void startService(){
        tailListener.setParams(datastore, collection, interpreter, filePath);
        Tailer tailer = new Tailer(new File(filePath), tailListener, defaultTime, startFromEnd);
        Thread thread = new Thread(tailer);
        thread.setDaemon(true);
        thread.start();
        this.tailer = tailer;
    }

    /**
     * stop the file watch service
     */
    public void stopService(){
        this.tailer.stop();
    }

}
