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

import com.blobcity.db.code.datainterpreter.InterpreterStoreBean;
import com.blobcity.db.constants.BSql;
import com.blobcity.db.exceptions.ErrorCode;
import com.blobcity.db.exceptions.OperationException;
import com.blobcity.db.sql.util.PathUtil;
import com.blobcity.lib.database.bean.manager.factory.ModuleApplicationContextHolder;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.File;
import java.util.*;

/**
 * Bean responsible for managing watch service operations
 *
 * @author sanketsarang
 */
@Component
public class WatchServiceManager {
    private static final Logger logger = LoggerFactory.getLogger(WatchServiceManager.class);

    private Map<String, FolderWatchService> folderMap;
    private Map<String, FileWatchService> fileWatchMap;
    @Autowired
    private ModuleApplicationContextHolder applicationContextHolder;
    @Autowired
    private InterpreterStoreBean interpreterStoreBean;

    @PostConstruct
    public void init(){
        folderMap = new HashMap<>();
        fileWatchMap = new HashMap<>();
    }

    public void startWatch(String relativePath, final String datastore, final String collection, final boolean startFromEnd, final WatchServiceImportType importType, final String interpreter) throws OperationException {

        String absolutePath = PathUtil.datastoreFtpFolder(datastore) + (relativePath.startsWith(BSql.SEPERATOR) ? "" : BSql.SEPERATOR) + relativePath;

        if(new File(absolutePath).isDirectory()) {
            startFolderWatch(relativePath, datastore, collection, startFromEnd, importType, interpreter);
        } else {
            if(importType == WatchServiceImportType.FILE) {
                throw new OperationException(ErrorCode.INVALID_QUERY_PARAMETER, "Import type of 'file' allowed only when watching folders");
            }
            startFileWatch(relativePath, datastore, collection, startFromEnd, interpreter);
        }
    }

    /**
     * Add a simple folder watch service. This doesn't control when to check folder for the changes.
     *
     * @param folderPath: folderPath to folder
     * @param datastore name of datastore
     * @param collection name of collection
     * @param startFromEnd true if file is to be read from end; false for reading from beginning
     * @param interpreter name of interpreter to use for each line of each file; null if none to be used
     * @throws OperationException:
     *  1. if folder is already watched
     *  2. given path is not to a folder
     *  3. given path/folder doesn't exist
     *  4. internal error happened in watch service registration
     */
    private void startFolderWatch(String folderPath, final String datastore, final String collection, final boolean startFromEnd, final WatchServiceImportType importType, final String interpreter) throws OperationException {

        if(folderPath.contains("..")) {
            throw new OperationException(ErrorCode.INVALID_QUERY_PARAMETER, "Relative paths not allowed. Consider using absolute path instead");
        }

        String absoluteFolderPath = PathUtil.datastoreFtpFolder(datastore) + (folderPath.startsWith(BSql.SEPERATOR) ? "" : BSql.SEPERATOR) + folderPath;

        if(!absoluteFolderPath.endsWith(BSql.SEPERATOR)) {
            absoluteFolderPath = absoluteFolderPath + BSql.SEPERATOR;
        }

        if(folderMap.containsKey(absoluteFolderPath)) {
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "This folder is already being watched");
        }

        if(!new File(absoluteFolderPath).isDirectory()) {
            throw new OperationException(ErrorCode.INVALID_QUERY_PARAMETER, "Given folderPath is not a folder");
        }

        if(!new File(absoluteFolderPath).exists()) {
            throw new OperationException(ErrorCode.INVALID_QUERY_PARAMETER, "Given folder cant't be found");
        }

        if(interpreter != null && !interpreterStoreBean.isPresent(datastore, interpreter)) {
            throw new OperationException(ErrorCode.INVALID_QUERY_PARAMETER, "No interpreter found with the given name: " + interpreter);
        }

        if(new File(absoluteFolderPath).isFile() && importType == WatchServiceImportType.FILE) {
            throw new OperationException(ErrorCode.INVALID_QUERY_PARAMETER, "Import type of 'file' can only be used when watching a folder");
        }

        if(new File(absoluteFolderPath).isFile() && importType == WatchServiceImportType.FILE_INTERPRETED) {
            throw new OperationException(ErrorCode.INVALID_QUERY_PARAMETER, "Import type of 'file-interpreted' can only be used when watching a folder");
        }

        FolderWatchService folderWatchService = applicationContextHolder.getApplicationContext().getBean(FolderWatchService.class);
        folderWatchService.setParams(absoluteFolderPath, datastore, collection, startFromEnd, importType, interpreter);
        folderWatchService.register();
        folderWatchService.startWatching(importType);
        folderMap.put(absoluteFolderPath, folderWatchService);
    }

    public void stopWatch(final String relativePath, final String datastore) throws OperationException {
        String absolutePath = PathUtil.datastoreFtpFolder(datastore) + (relativePath.startsWith(BSql.SEPERATOR) ? "" : BSql.SEPERATOR) + relativePath;

        if(new File(absolutePath).isDirectory()) {
            stopFolderWatch(relativePath, datastore);
        } else {
            stopFileWatch(relativePath, datastore);
        }
    }

    /**
     * Remove the service associated with given path
     *
     * @param path: full path to folder
     * @param ds name of datastore
     * @throws OperationException:
     *      1. if folder is not being watched
     *      2. if internal error occurred during stopping the execution
     */
    private void stopFolderWatch(String path, final String ds) throws OperationException {
        String absoluteFolderPath = PathUtil.datastoreFtpFolder(ds) + (path.startsWith(BSql.SEPERATOR) ? "" : BSql.SEPERATOR) + path;
        if(!absoluteFolderPath.endsWith(BSql.SEPERATOR))
            absoluteFolderPath = absoluteFolderPath + BSql.SEPERATOR;
        if( !folderMap.containsKey(absoluteFolderPath) )
            throw new OperationException(ErrorCode.INVALID_QUERY_PARAMETER, "Folder " + path + "is not being watched");
        folderMap.get(absoluteFolderPath).close();
        folderMap.remove(absoluteFolderPath);
    }

    /**
     * Get the list of the watched folders with files inside them as
     *
     * @return: JSONArray where each element is a JSONObject for a folder under watch
     */
    public JSONArray listFolderWatchJSON(){
        JSONArray array = new JSONArray();
        for(String folder:  folderMap.keySet()){
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("folder", folder);
            jsonObject.put("files", folderMap.get(folder).listFilesUnderWatchShort());
            array.put(jsonObject);
        }
        return array;
    }

    /**
     * List all folder watch services
     *
     * @return: List of all the folders which are currently being watched
     */
    public List<String> listFolderWatches(){
        return new ArrayList<>(folderMap.keySet());
    }

    /**
     * This function will initialize all the services active before database reboot.
     *
     */
    public void initializeOlderServices(){
        // get all services from the table here and initialize them one by one
    }

    /* File Watch Related Functions */

    /**
     * Start a file watch service and read all the existing lines in the file as well
     *
     * @param filePath: full path to file
     * @param datastore name of datastore
     * @param collection name of collection
     * @param interpreter name of interpreter to use for each line of file; null if none to be used
     */
    @Deprecated //use the one with the startFromEnd parameter
    public void startFileWatch(final String filePath, final String datastore, final String collection, final String interpreter) throws OperationException{
        startFileWatch(filePath,datastore, collection, false, interpreter);
    }

    /**
     * Start a file watch service with option to choose the watch service from the beginning or end of file
     *
     * @param filePath: full path to file
     * @param startFromEnd:
     *      true: watch file for all further changes only
     *      false: read the existing lines also
     * @param interpreter name of interpreter to use per line of file; null if none to be used
     */
    public void startFileWatch(final String filePath, final String datastore, final String collection, boolean startFromEnd, final String interpreter) throws  OperationException{

        if(filePath.contains("..")) {
            throw new OperationException(ErrorCode.INVALID_QUERY_PARAMETER, "Relative paths not allowed. Consider using absolute path instead");
        }

        final String absoluteFilePath = PathUtil.datastoreFtpFolder(datastore) + (filePath.startsWith(BSql.SEPERATOR) ? "" : BSql.SEPERATOR) + filePath;

        // a new bean instance for every call bcoz it is essential to stop each file watch individually
        FileWatchService fileWatchService = applicationContextHolder.getApplicationContext().getBean(FileWatchService.class);
        if(fileWatchMap.containsKey(absoluteFilePath))
            throw new OperationException(ErrorCode.INVALID_QUERY_PARAMETER, "This file is already being watched");
        if(!new File(filePath).exists())
            throw new OperationException(ErrorCode.INVALID_QUERY_PARAMETER, "Specified file doesn't exist");
        fileWatchService.setOptions(absoluteFilePath, datastore, collection, startFromEnd, interpreter);
        fileWatchService.startService();

        fileWatchMap.put(absoluteFilePath, fileWatchService);
    }

    /**
     * Stop the watch service for given file
     *
     * @param filePath: full path to file being under watch_
     * @throws OperationException
     */
    public void stopFileWatch(final String filePath, final String ds) throws  OperationException{
        final String absoluteFilePath = PathUtil.datastoreFtpFolder(ds) + (filePath.startsWith(BSql.SEPERATOR) ? "" : BSql.SEPERATOR) + filePath;
        if(!fileWatchMap.containsKey(absoluteFilePath))
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "Specified file is not being watched");

        FileWatchService fileWatchServiceObj = fileWatchMap.get(absoluteFilePath);
        fileWatchServiceObj.stopService();
        fileWatchMap.remove(absoluteFilePath);
        logger.info("File " + filePath + " is no longer being watched");
    }

    /**
     * Get the list of independent files (files which are not present in a watched folder) being watched
     *
     * @return: list of files currently watched independently
     */
    public List<String> listFileWatches(){
        return new ArrayList<>(fileWatchMap.keySet());
    }

    /**
     * Get the list of files in a under watch folder
     *
     * @param folderPath: path to folder being watched
     * @return: list of files with simple names only
     * @throws OperationException
     */
    public List<String> listFilesUnderFolderWatch(String folderPath) throws  OperationException{
        if(!folderPath.endsWith(BSql.SEPERATOR))
            folderPath = folderPath+BSql.SEPERATOR;
        if(!folderMap.containsKey(folderPath)){
            throw new OperationException(ErrorCode.INVALID_QUERY_PARAMETER, folderPath + " is not being watched");
        }
        return folderMap.get(folderPath).listFilesUnderWatchShort();
    }
}

