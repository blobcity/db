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

package com.blobcity.db.versioning;

import com.blobcity.db.bsql.BSqlIndexManager;
import com.blobcity.db.constants.BSql;
import com.blobcity.db.exceptions.OperationException;
import com.blobcity.db.schema.*;
import com.blobcity.db.schema.beans.SchemaManager;
import com.blobcity.db.sql.util.PathUtil;
import com.blobcity.db.storage.BSqlFileManager;
import com.blobcity.db.util.FileNameEncoding;
import com.blobcity.db.util.SystemInputUtil;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.logging.Level;

/**
 * //TODO: YET TO IMPLEMENT THE MIGRATION LOGIC. DO NOT INVOKE THIS UNLESS IMPLEMENTATION IS COMPLETED.
 *
 * This is used to upgrade to storage version 5 from storage version 4
 * 
 * Things to take care of in this version:
 * 1. Sets _id field to all entries in the user table to make user login password editable.
 *
 * @author sanketsarang
 */
public class Version4to5 implements VersionUpgrader{
    
    private static final Logger logger = LoggerFactory.getLogger(Version4to5.class);
    
    private static final String OLD_VERSION = "4";
    private static final String NEW_VERSION = "5";
    private static final String BACKUP_FILE_POSTFIX = "-v4.zip";
        
    // folders to skip in upgrading.
    private final Set<String> skipAppLevelFolders = new HashSet<>(Arrays.asList(new String[]{"global-live", "global-del", "BlobCityDB"}));

    @Override
    public void upgrade() {
        logger.info("Upgrading from " + OLD_VERSION + " to new version " + NEW_VERSION + " . . . ");
        try{
            //TODO: erorr in creating zip of the folder.
            // needs to be  checked
//            performBackup();
            
            startUpgrade();
            
//            removeBackup();
        } catch(Exception ex){
            logger.warn("Data storage format upgrade to version " + NEW_VERSION + " failed. The application may not function correctly.");
            logger.error(null, ex);
        }
    }
    
    public void performBackup(){
        try {
            logger.info("Archiving current data for backup");
            System.out.print("Zipping .");
            final String baseFolder = BSql.BSQL_BASE_FOLDER.substring(0, BSql.BSQL_BASE_FOLDER.length() - 1);
            new FolderZipper().zipFolder(baseFolder, baseFolder + BACKUP_FILE_POSTFIX);
            logger.info("Archieving complete");
        } catch (IOException ex) {
            boolean yes = SystemInputUtil.captureYesNoInput("Archieving old data failed with unknown cause. "
                    + "It is highly recommended that you archieve your existing data before you proceed with "
                    + "upgrading your data store. Would you like to proceed without creating a backup? (y/n)");
            if (!yes) {
                logger.info("Quitting BlobCity DB due to archive error before version upgrade");
                System.exit(0);
            } else {
                logger.info("Proceeding with data store migration to version " + NEW_VERSION + " without data backup");
            }
        }
    }
    
    public void removeBackup(){
        logger.info("Removing backup archieve");
        final String backupFile = BSql.BSQL_BASE_FOLDER.substring(0, BSql.BSQL_BASE_FOLDER.length() - 1) + BACKUP_FILE_POSTFIX;
        try {
            Files.deleteIfExists(FileSystems.getDefault().getPath(backupFile));
        } catch (IOException ex) {
            logger.error("Failed to remove backup archieve at " + backupFile
                    + ". You may manually delete the file in order to save disk space.");
            java.util.logging.Logger.getLogger(Version1to2.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    public void startUpgrade(){
        upgradeStructure();        
    }
    
    /**
     * NOTE:
     * upgrade database structure
     * create user groups table
     * 
     * NOTE: this can't be done as there is dependency issue if we try to autoWire bean UserGroupManager here
     * SO this is now done, with the help of configBean now
     */
    public void upgradeStructure(){
        
        //TODO: Update the Users table to include the _id primary key for each user
    }


    private JSONObject readRecordFile(final String ds, final String collection, final String pk) {
        BSqlFileManager fileManager = new BSqlFileManager();
        JSONObject fileJson;
        try {
            return new JSONObject(fileManager.select(ds, collection, pk));
        } catch (JSONException ex) {
            logger.error("Error in reading old record with primary key " + pk + " during version upgrade of "
                    + ds + "." + collection);
        } catch (OperationException ex) {
            logger.error("Error in reading old record with primary key " + pk + " during version upgrade of "
                    + ds + "." + collection);
        }

        return null;
    }

    private void deleteRecordFile(final String ds, final String collection, final String pk) {
        BSqlFileManager fileManager = new BSqlFileManager();
        try {
            fileManager.remove(ds, collection, pk);
        } catch (OperationException ex) {
            logger.error("Error in deleting old record with primary key " + pk + " during version upgrade of "
                    + ds + "." + collection);
        }
    }

    private void writeRecordFile(final String ds, final String collection, final String pk, final JSONObject internalRecord) {
        BSqlFileManager fileManager = new BSqlFileManager();
        try {
            fileManager.insert(ds, collection, pk, internalRecord.toString());
        } catch (OperationException ex) {
            logger.error("Error in inserting new record with primary key " + pk + " during version upgrade of "
                    + ds + "." + collection);
        }
    }
    
}
