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

package com.blobcity.db.bsql;

import com.blobcity.db.code.CodeLoader;
import com.blobcity.db.code.ManifestLang;
import com.blobcity.db.constants.BSql;
import com.blobcity.db.exceptions.ErrorCode;
import com.blobcity.db.exceptions.OperationException;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;

import com.blobcity.db.operations.OperationExecutor;
import com.sun.org.apache.xpath.internal.operations.Operation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

/**
 *
 * @author sanketsarang
 */
@Component
public class BSqlDatastoreManager {
    
    @Autowired @Lazy
    private CodeLoader codeloader;
    
    Logger logger = LoggerFactory.getLogger(BSqlDatastoreManager.class);
    
    public void createDatastore(final String datastoreName) throws OperationException {
        
        if(exists(datastoreName)) {
            throw new OperationException(ErrorCode.DATASTORE_ALREADY_EXISTS);
        }

        createDirectories(datastoreName);
        
        //TODO: Make other required folders here.
        //TODO: create the Manifest File here

    }
    
    public boolean exists(final String databaseName) {
        return new File(BSql.BSQL_BASE_FOLDER + databaseName).exists();
    }
    
    public List<String> listDatabases(){
        File baseFolder = new File(BSql.BSQL_BASE_FOLDER);
        List<String> dbs = new ArrayList<>();
        for(File file: baseFolder.listFiles()){
            if(file.isDirectory()
                    && !file.getName().contains("global")
                    && !file.getName().equals(".systemdb")
                    && !file.getName().contains("commit-logs")
                    && !file.getName().equals(".ftpusers")){
                dbs.add(file.getName());
            }
        }
        return dbs;
    }

    /**
     * Drops a datastore without possibility of rolling back the operation. This function is for internal use by the
     * database only and show not be invoke-able by user queries.
     *
     * @param ds name of datastore
     * @throws OperationException if an error occurs
     */
    public void dropDsNoArchive(final String ds) throws OperationException {
        if( ! this.exists(ds)){
            return; //provide a success response if a dsSet with the specified name is inexistent
        }
        // remove entry for all custom codes by user
        codeloader.removeAllClasses(ds);
        // TODO: remove from other in-memory location like indexing and caching etc.

        final String absolutePath = BSql.BSQL_BASE_FOLDER + ds;
        try {
            Files.deleteIfExists(FileSystems.getDefault().getPath(absolutePath));
        } catch (IOException ex) {
            logger.error("Error during dropping database " + ds + ex.getMessage() );
            throw new OperationException(ErrorCode.DATASTORE_DELETION_ERROR);
        }
    }
    
    /**
     * Remove all data inside the database and database itself
     * 
     * @param ds name of the datastore
     * @param archiveCode the code under which the data should be archived
     * @throws OperationException 
     */
    public void dropDatabase(final String ds, final String archiveCode) throws OperationException{
        if( ! this.exists(ds)){
            return; //provide a success response if a dsSet with the specified name is inexistent
        }
        // remove entry for all custom codes by user
        codeloader.removeAllClasses(ds);
        // TODO: remove from other in-memory location like indexing and caching etc.

        String absolutePath = BSql.BSQL_BASE_FOLDER + ds;
        final String archieveCode = ds + "." + System.currentTimeMillis();
        String backupPath = BSql.GLOBAL_DELETE_FOLDER + archiveCode + BSql.SEPERATOR;
        try {
            new File(backupPath).mkdir();
            backupPath += ds;
            Files.move(FileSystems.getDefault().getPath(absolutePath), FileSystems.getDefault().getPath(backupPath), StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException ex) {
            logger.error("Error during dropping database " + ds + ex.getMessage() );
            throw new OperationException(ErrorCode.DATASTORE_DELETION_ERROR);
        }
    }

    /**
     * Undo's a drop-ds operation that is alread performed
     * @param archiveCode the archive code of the archive that idenfies the data to be restored
     * @throws OperationException if an error occures in performing the restore
     */
    public void undoDropDs(String archiveCode) throws OperationException {
        throw new UnsupportedOperationException("Not supported yet.");
    }
    
    /**
     * This will remove all the data inside it like codes, and all the tables but database is still there with no tables and other things
     * 
     * @param ds name of datastore
     * @param achieveCode the unique archive code
     * @throws OperationException 
     */
    public void truncateDs(String ds, final String archiveCode) throws OperationException {
        dropDatabase(ds, archiveCode);
        createDirectories(ds);
    }

    /**
     * Undo's a truncate DS operation that is already performed
     * @param archiveCode the archive code of the archive that identifies the data to be restored
     * @throws OperationException if any error occurres in performing the restore
     */
    public void undoTruncateDs(String archiveCode) throws OperationException {

        //TODO: Implement this

        throw new UnsupportedOperationException("Not supported yet.");
    }
    
    /**
     * Create required directories for a database
     * @param databaseName 
     */
    private void createDirectories(String databaseName){
        String databaseLocation = BSql.BSQL_BASE_FOLDER + databaseName + BSql.SEPERATOR;
        new File(databaseLocation).mkdir();
        new File(databaseLocation + BSql.SEPERATOR + BSql.DATABASE_FOLDER_NAME).mkdir();
        new File(databaseLocation + BSql.DELETE_FOLDER).mkdir();
        new File(databaseLocation + BSql.DB_HOT_DEPLOY_FOLDER).mkdir();
        new File(databaseLocation + BSql.EXPORT_FOLDER).mkdir();
        new File(databaseLocation + BSql.IMPORT_FOLDER).mkdir();
        new File(databaseLocation + BSql.MAP_REDUCE_FOLDER).mkdir();
        new File(databaseLocation + BSql.COMMIT_LOGS_FOLDER).mkdir();
        new File(databaseLocation + BSql.FTP_FOLDER_NAME).mkdir();
    }

    /**
     * Create the basic manifest file inside the deploy db-hot folder of given dsSet
     *
     * @param datastore : dsSet name
     * @throws  OperationException : if the manifest file is not created
     */
    public void createManifestFile(final String datastore) throws OperationException {
        String fileLocation  = BSql.BSQL_BASE_FOLDER + datastore + BSql.SEPERATOR + BSql.DB_HOT_DEPLOY_FOLDER;
        try {
            new File("db-code.mf").createNewFile();
            BufferedWriter bw = new BufferedWriter(new FileWriter(fileLocation+"db-code.mf"));
            bw.write(ManifestLang.VERSION.getLiteral()+": 1.0");
            bw.newLine();
            bw.write(ManifestLang.PROCEDURES.getLiteral());
            bw.newLine();
            bw.write(ManifestLang.FILTERS.getLiteral());
            bw.newLine();
            bw.write(ManifestLang.TRIGGERS.getLiteral());
            bw.newLine();
            bw.write(ManifestLang.DATAINTERPRETERS.getLiteral());
            bw.newLine();
        } catch (IOException e) {
            e.printStackTrace();
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "Error in creating Manifest file");
        }

    }
}
