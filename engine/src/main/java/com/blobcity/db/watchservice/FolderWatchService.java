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

import com.blobcity.db.apache.tikka.TikaInterpreterService;
import com.blobcity.db.bquery.SQLExecutorBean;
import com.blobcity.db.bsql.BSqlDataManager;
import com.blobcity.db.code.CodeExecutor;
import com.blobcity.db.constants.BSql;
import com.blobcity.db.exceptions.ErrorCode;
import com.blobcity.db.exceptions.OperationException;
import com.blobcity.db.lang.Operators;
import com.blobcity.db.memory.records.JsonRecord;
import com.blobcity.db.requests.RequestHandlingBean;
import com.blobcity.lib.data.Record;
import com.blobcity.lib.database.bean.manager.factory.ModuleApplicationContextHolder;
import com.blobcity.lib.query.Query;
import com.blobcity.lib.query.RecordType;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.OVERFLOW;
import static java.nio.file.StandardWatchEventKinds.ENTRY_DELETE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY;

/**
 * A folder watch service bean
 *
 * @author sanketsarang
 */
@Component
public class FolderWatchService{

    private static final Logger logger = LoggerFactory.getLogger(FolderWatchService.class);

    private String folderPath;
    private Boolean infinite ;
    private WatchService watcher;
    private Integer TIME_FREQ;
    private TimeUnit TIME_UNIT;
    private String datastore;
    private String collection;
    private Boolean startFromEnd;
    private WatchServiceImportType importType;
    private String interpreter;

    @Autowired
    private ModuleApplicationContextHolder applicationContextHolder;
    @Autowired
    private RequestHandlingBean requestHandlingBean;
    @Autowired
    private CodeExecutor codeExecutor;
    @Autowired
    private BSqlDataManager dataManager;
    @Autowired
    private SQLExecutorBean sqlExecutorBean;

    private Map<String, FileWatchService> fileWatchServiceMap;

    @PostConstruct
    public void init(){
        this.TIME_FREQ = 10;
        this.TIME_UNIT = TimeUnit.MINUTES;
        this.startFromEnd = false;
        this.infinite = true;
        fileWatchServiceMap = new HashMap<>();
    }

    /**
     * Schedule the service to check after user-given time
     *
     * @param value : time value after which folder is checked
     * @param unit: time unit (s,m,h,d)
     */
    public void setFrequency(final Integer value, final String unit){
        switch (unit){
            case "s":
                this.TIME_UNIT = TimeUnit.SECONDS;
            case "m":
                this.TIME_UNIT = TimeUnit.MINUTES;
            case "h":
                this.TIME_UNIT = TimeUnit.HOURS;
            case "d":
                this.TIME_UNIT = TimeUnit.DAYS;
            default:
                this.TIME_UNIT = TimeUnit.MINUTES;
        }
        // 0 value is not allowed for anything
        this.TIME_FREQ = (value==0) ? TIME_FREQ : value;
    }

    /**
     * Set required parameters to a folder watch service
     *
     * @param folderPath: path to folder
     * @param datastore: dsSet name
     * @param collection: collection name
     * @param startFromEnd: whether to start from the end for existing files
     * @param interpreter name of interpreter to use for each line of each file; null if none to be used
     */
    public void setParams(final String folderPath, final String datastore, final String collection, final boolean startFromEnd, final WatchServiceImportType importType, final String interpreter){
        this.folderPath = folderPath;
        this.datastore = datastore;
        this.collection = collection;
        this.startFromEnd = startFromEnd;
        this.importType = importType;
        this.interpreter = interpreter;
    }

    /**
     * Register the folder watch service
     *
     * @throws OperationException
     */
    public void register() throws OperationException{
        try {
            watcher = FileSystems.getDefault().newWatchService();
            Paths.get(this.folderPath).register(watcher, ENTRY_CREATE, ENTRY_DELETE, ENTRY_MODIFY);
        } catch (IOException e) {
            logger.error(null, e);
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR);
        }
    }

    /**
     * Close the folder watch service here
     *
     * @throws OperationException
     */
    public void close() throws OperationException {
        stopAllFileWatches();
        this.infinite = false;
        try {
            this.watcher.close();
        } catch (IOException e) {
            logger.error(null, e);
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR);
        }
    }

    /**
     * Start watching the folder. This function handles all the change events
     */
    public void startWatching(WatchServiceImportType importType) throws OperationException{
        logger.debug("Watching Folder now" + this.folderPath.toString());
        // read exisiting files first
        startFileWatchForExistingFiles(importType);
        /* Check for new file events */
        Thread t = new Thread(() -> {
            try {
                WatchKey key ;
                while(infinite){
                    key = watcher.take();
                    for(WatchEvent<?> event: key.pollEvents()){
                        WatchEvent.Kind<?> kind = event.kind();
                        // check what kind of event is this
                        if(OVERFLOW == kind) {
                            //TODO: Manually find a delta of the changes

                            continue;
                        }
                        else if (kind == ENTRY_CREATE)
                            fileCreateEvent(this.folderPath + event.context().toString());
                        else if (kind == ENTRY_DELETE)
                            fileDeleteEvent(this.folderPath + event.context().toString());
                        else if(kind == ENTRY_MODIFY)
                            fileModifyEvent(this.folderPath + event.context().toString());
                    }
                    // if the key is invalid, break;
                    if(!key.reset()) break;
                }
            } catch (InterruptedException e) {
                logger.error("Error in folder watch service for folder: "+ this.folderPath.toString() , e);
                e.printStackTrace();
            }
        });
        t.start();
    }

    /**
     * function for handling a file CREATE event in a folder being watched
     * Add a new file watch service
     *
     * @param filePath: path of file which was modified
     */
    public void fileCreateEvent(final String filePath){
        logger.debug("CREATED File: " + filePath);
        if(!new File(filePath).isDirectory() && new File(filePath).canWrite()) {
            switch(importType) {
                case LINE:
                    startTailFile(filePath, false, interpreter);
                    break;
                case FILE:
                    readWholeFile(filePath, interpreter);
                    break;
                case FILE_INTERPRETED:
                    readWholeFileInterpreted(filePath, interpreter);
                    break;
            }
        }
    }

    /**
     * What happens if a file in the folder is deleted
     *
     * @param filePath: path of file which was modified
     */
    public void fileDeleteEvent(final String filePath){
        logger.debug("DELETED: " + filePath);
        if(!new File(filePath).isDirectory()) {
            switch(importType) {
                case LINE:
                    stopTail(filePath);
                    break;
                case FILE:
                    //do nothing
                    break;
            }
        }
    }

    /**
     * function for handling a file MODIFY event in a folder being watched
     *
     * @param filePath: path of file which was modified
     */
    public void fileModifyEvent(final String filePath){
        logger.debug("MODIFIED: " + filePath);

        if(!new File(filePath).isDirectory() && new File(filePath).canWrite()) {
            switch(importType) {
                case LINE:
                    //to implement
                    break;
                case FILE:
                    //to implement
                    break;
                case FILE_INTERPRETED:
                    readWholeFileInterpreted(filePath, interpreter);
                    break;
            }
        }
    }

    /**
     * This function will initialize the file tail service for all the files present in the folder under watch
     */
    public void startFileWatchForExistingFiles(WatchServiceImportType importType){
        for(File fp: new File(folderPath).listFiles()){
            if(!fp.isDirectory()) {
                switch(importType) {
                    case LINE:
                        startTailFile(fp.getAbsolutePath(), this.startFromEnd, interpreter);
                        break;
                    case FILE:
                        readWholeFile(fp.getAbsolutePath(), interpreter);
                        break;
                }
            }

        }
    }

    /**
     * stop tailing all the files in the folder
     */
    public void stopAllFileWatches(){
        for(String path: fileWatchServiceMap.keySet()){
            fileWatchServiceMap.get(path).stopService();
        }
        fileWatchServiceMap = new HashMap<>();
    }

    /**
     * Start watch service for given file
     *
     * @param filePath: full path to file
     * @param startFromEnd: whether to start tailing from end or read all from beginning
     * @param interpreter name of interpreter to use for every line; null if none to be used
     */
    public void startTailFile(final String filePath, final Boolean startFromEnd, final String interpreter){
        if(fileWatchServiceMap.containsKey(filePath)) return;
        FileWatchService fileWatchService = applicationContextHolder.getApplicationContext().getBean(FileWatchService.class);
        fileWatchService.setOptions(filePath, this.datastore, this.collection, startFromEnd, interpreter);
        fileWatchService.startService();
        fileWatchServiceMap.put(filePath, fileWatchService);
    }

    public void readWholeFile(final String filePath, final String interpreter) {
        File file = new File(filePath);
        FileInputStream fis = null;
        byte []data;
        try {
            fis = new FileInputStream(file);
            data = new byte[(int) file.length()];
            fis.read(data);
        } catch (IOException e) {
            e.printStackTrace();
            return;
        } finally {
            if(fis != null) {
                try {
                    fis.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        final String fileContents;
        try {
            fileContents = new String(data, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            return;
        }

        JSONObject rowJson;
        if(interpreter == null) {
            rowJson = new JSONObject();
            rowJson.put("_file", filePath);
            rowJson.put("_txt", fileContents);
        } else {
            try {
                rowJson = codeExecutor.executeDataInterpreter(datastore, interpreter, fileContents);
            } catch (OperationException ex) {
                logger.error("error in running " + interpreter + " for " + datastore + "." + collection + " watch service for file contents: " + fileContents);
                rowJson = new JSONObject();
            }
        }

        logger.debug(filePath + ","+datastore+","+collection+":"+rowJson.toString());
        Query query = new Query().insertQuery(datastore, collection, Arrays.asList(new Record[]{new JsonRecord(rowJson)}), RecordType.JSON);
        requestHandlingBean.newRequest(query);
    }

    public void readWholeFileInterpreted(final String filePath, final String interpreter) {
        JSONObject json;

        try {
            json = new TikaInterpreterService().toJson(filePath);
        } catch (OperationException e) {
            logger.warn("error in watch-service file import for file: " + filePath);
            return;
        }

        if(json.getString("_txt").isEmpty()) {
            return;
        }

        json.put("_file", filePath);

        /* Search for an existing record for the file, and perform a update instead of insert if a record is found */
        try {
            //TODO: this code needs to be made cluster compliant
            JSONObject sqlResponse = new JSONObject(sqlExecutorBean.executePrivileged(datastore, "select * from `" + datastore + "`.`" + collection + "` where `_file`='" + filePath + "'"));
            JSONArray records = sqlResponse.getJSONArray("p");

            if(records.length() > 1) {
                return;
            }

            if(records.length() == 1) {
                JSONObject existingJson = records.getJSONObject(0);
                json.put("_id", existingJson.getString("_id"));
                dataManager.save(datastore, collection, json);
                return;
            }
        } catch (OperationException e) {
            //do nothing
        }

        if(interpreter != null && !interpreter.isEmpty()) {
            try {
                json = codeExecutor.executeDataInterpreter(datastore, interpreter, json.toString());
            } catch (OperationException ex) {
                logger.error("error in running " + interpreter + " for " + datastore + "." + collection + " watch service for file contents: " + json.toString());
                json = new JSONObject();
            }
        }


        logger.debug(filePath + ","+datastore+","+collection+":"+json.toString());
        Query query = new Query().insertQuery(datastore, collection, Arrays.asList(new Record[]{new JsonRecord(json)}), RecordType.JSON);
        requestHandlingBean.newRequest(query);
    }

    /**
     * stop watch service for given file
     *
     * @param filePath: full path to file
     */
    public void stopTail(final String filePath){
        if(!fileWatchServiceMap.containsKey(filePath)) return;
        fileWatchServiceMap.get(filePath).stopService();
        fileWatchServiceMap.remove(filePath);
    }

    /**
     * Get the list of all files being watched under this folder
     *
     * @return: List of files being watched in this folder with full path
     */
    public List<String> listFilesUnderWatch(){
        return new ArrayList<>(fileWatchServiceMap.keySet());
    }

    /**
     * Get the list of all files simple name being watched under this folder
     *
     * @return: list of simple file names
     */
    public List<String> listFilesUnderWatchShort(){
        ArrayList<String> tmp = new ArrayList<>();
        for(String fp: fileWatchServiceMap.keySet()){
            String[] ff = fp.split(BSql.SEPERATOR);
            tmp.add(ff[ff.length-1]);
        }
        return tmp;
    }
}
