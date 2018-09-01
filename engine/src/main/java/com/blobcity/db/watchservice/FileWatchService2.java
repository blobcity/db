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

import com.blobcity.db.exceptions.ErrorCode;
import com.blobcity.db.exceptions.OperationException;
import org.springframework.stereotype.Component;

import java.io.*;
import java.nio.Buffer;

/**
 * @author sanketsarang
 */
@Component
public class FileWatchService2 implements Runnable{

    // path of file
    private String filePath;
    // a reader for a file
    private BufferedReader reader;

    private long lastReadLine;


    public FileWatchService2(){
        lastReadLine = 0;
    }

    public void initialize(String path) throws OperationException{

        try{
            this.reader = new BufferedReader(new FileReader(path));
        }catch (FileNotFoundException ex){
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "Given file doesn't exist");
        }
        this.filePath = path;
        this.lastReadLine = 0;
    }

    public void initialize(String path, long lastReadLine) throws OperationException{
        try{
            this.reader = new BufferedReader(new FileReader(path));
        }catch (FileNotFoundException ex){
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "Given file doesn't exist");
        }
        this.filePath = path;
        this.lastReadLine = lastReadLine;
    }


    public void startWatch(String path){
        Thread thread = new Thread(this);
        thread.setDaemon(true);
        thread.start();
    }


    public void run() {
        try {
            RandomAccessFile readerr = new RandomAccessFile(new File(this.filePath), "r");
        } catch (FileNotFoundException e) {
            // throw Exception
        }
    }
}
