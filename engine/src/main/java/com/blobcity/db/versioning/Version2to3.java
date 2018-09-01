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

import com.blobcity.db.constants.BSql;
import com.blobcity.db.util.SystemInputUtil;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.NotDirectoryException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 *
 * @author sanketsarang
 */
@Component
public class Version2to3 implements VersionUpgrader {

    private static final String OLD_VERSION = "1.2";
    private static final String NEW_VERSION = "1.3";
    private static final String BACKUP_FILE_POSTFIX = "-v2.zip";
    private static final Logger logger = LoggerFactory.getLogger(Version1to2.class.getName());
    int zipCount = 0;
    private final Set<String> skipAppLevelFolders = new HashSet<>(Arrays.asList(new String[]{"global-live", "global-del", "BlobCityDB"}));

    @Override
    public void upgrade() {
        try {
            logger.info("Upgrading version from " + OLD_VERSION + " to " + NEW_VERSION + " . . .");

            /* Create backup */
            createBackup();

            /* Perform actual data upgrade */
            upgradeData();

            /* Remove backup copy */
            removeBackup();

        } catch (Exception ex) {
            logger.error("Data storage format upgrade to version " + NEW_VERSION + " failed. The application may not function correctly.");
            java.util.logging.Logger.getLogger(Version1to2.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private static String sourceApp = null;
    private static String destinationApp = null;

    /**
     * Gets the number of files in the specified folder. Symbolic links if any are ignored.
     *
     * @param dir
     * @return
     * @throws IOException
     * @throws NotDirectoryException
     */
    private int getFilesCount(Path dir) throws IOException, NotDirectoryException {
        int c = 0;
        if (Files.isDirectory(dir)) {
            try (DirectoryStream<Path> files = Files.newDirectoryStream(dir)) {
                for (Path file : files) {
                    if (Files.isRegularFile(file) || Files.isSymbolicLink(file)) {
                        // symbolic link also looks like file
                        c++;
                    }
                }
            }
        } else {
            throw new NotDirectoryException(dir + " is not directory");
        }

        return c;
    }

    private void createBackup() {
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

    private void upgradeData() {
        try {
            DirectoryStream<Path> ds = Files.newDirectoryStream(FileSystems.getDefault().getPath(BSql.BSQL_BASE_FOLDER));
            ds.forEach(path -> {
                try {
                    if (!skipAppLevelFolders.contains(path.getFileName().toString()) && Files.isDirectory(path)) {
                        portApplication(path.getFileName().toString());
                    }
                } catch (Exception ex) {
                    logger.info("Quitting BlobCity DB as upgrade to new version failed. It is recommended that you "
                            + "manually restore the data store from the archive before attempting a restart.", ex);
                    System.exit(0);
                }
            });
        } catch (IOException ex) {
            java.util.logging.Logger.getLogger(Version2to3.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private void portApplication(final String appId) {
        logger.info("Upgrading application " + appId);

        /* Process every table */
        try {
            DirectoryStream<Path> stream = Files.newDirectoryStream(FileSystems.getDefault().getPath(BSql.BSQL_BASE_FOLDER + appId + BSql.SEPERATOR + BSql.DATABASE_FOLDER_NAME));
            stream.iterator().forEachRemaining(path -> {
                if (!path.toFile().isDirectory()) {
                    return;
                }

                processTable(appId, path.getFileName().toString());
            });
        } catch (IOException ex) {
            java.util.logging.Logger.getLogger(Version2to3.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private void processTable(final String appId, final String table) {
        JSONObject oldSchemaJson;
        JSONObject newSchemaJson;

        /* Update schema to new format */
        try {
            oldSchemaJson = new JSONObject(Files.readAllLines(FileSystems.getDefault().getPath(BSql.BSQL_BASE_FOLDER + appId + BSql.SEPERATOR + BSql.DATABASE_FOLDER_NAME + BSql.SEPERATOR + table + BSql.SCHEMA_FILE)).get(0));
        } catch (IOException ex) {
            logger.error("Upgrade of " + appId + "." + table + " failed. The table may not function correctly.", ex);
            return;
        }

        /* Skip table upgrade if schema is already in new format */
        boolean skipSchema = false;
        for (Object key : oldSchemaJson.keySet()) {
            if ("primary".equals((String) key)) {
                continue;
            }

            if (oldSchemaJson.getJSONObject((String) key).get("type") instanceof JSONObject) {
                logger.info("Skipping " + appId + "." + table + " schema migration as it is already in new format");
                skipSchema = true;
                break;
            }
        }

        /* Create new schema. Converts the type parameter of every column from string to json. */
        if (!skipSchema) {
            newSchemaJson = new JSONObject();
            for (Object key : oldSchemaJson.keySet()) {
                if ("primary".equals((String) key)) {
                    newSchemaJson.put((String) key, oldSchemaJson.get((String) key));
                    continue;
                }

                JSONObject columnJson = oldSchemaJson.getJSONObject((String) key);
                columnJson.put("type", getMappingType(columnJson.getString("type")));
                newSchemaJson.put((String) key, columnJson);
            }

            /* Save new schema to schema file */
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(BSql.BSQL_BASE_FOLDER + appId + BSql.SEPERATOR + BSql.SEPERATOR + BSql.DATABASE_FOLDER_NAME + BSql.SEPERATOR + table + BSql.SCHEMA_FILE))) {
                writer.write(newSchemaJson.toString());
            } catch (IOException ex) {
                logger.error("Upgrade of " + appId + "." + table + " failed. The table may not function correctly.", ex);
            }
        }

        /* Add row count file */
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(BSql.BSQL_BASE_FOLDER + appId + BSql.SEPERATOR + BSql.DATABASE_FOLDER_NAME + BSql.SEPERATOR + table + BSql.TABLE_ROW_COUNT_FILE))) {
            long rowCount = getFilesCount(FileSystems.getDefault().getPath(BSql.BSQL_BASE_FOLDER + appId + BSql.SEPERATOR + BSql.DATABASE_FOLDER_NAME + BSql.SEPERATOR + table + BSql.DATA_FOLDER));
            writer.write("" + rowCount);
        } catch (IOException ex) {
            logger.error("Upgrade of " + appId + "." + table + " failed for row count specification. The row count value may be corrupted.", ex);
        }
    }

    private void removeBackup() {
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

    private static JSONObject getMappingType(final String type) {
        JSONObject jsonObject = new JSONObject();
        switch (type.toLowerCase()) {
            case "list<string>":
                jsonObject.put("type", "ARRAY");
                jsonObject.put("sub-type", "STRING");
                return jsonObject;
            case "list<int>":
                jsonObject.put("type", "ARRAY");
                jsonObject.put("sub-type", "INT");
                return jsonObject;
            case "list<long>":
                jsonObject.put("type", "ARRAY");
                jsonObject.put("sub-type", "BIGINT");
                return jsonObject;
            case "list<float>":
                jsonObject.put("type", "ARRAY");
                jsonObject.put("sub-type", "FLOAT");
                jsonObject.put("precision", "6");
                return jsonObject;
            case "list<double>":
                jsonObject.put("type", "ARRAY");
                jsonObject.put("sub-type", "DOUBLE");
                return jsonObject;
            case "list<char>":
                jsonObject.put("type", "ARRAY");
                jsonObject.put("sub-type", "CHAR");
                jsonObject.put("length", -1); 
                return jsonObject;
            case "string":
                jsonObject.put("type", "STRING");
                return jsonObject;
            case "int":
                jsonObject.put("type", "INT");
                return jsonObject;
            case "long":
                jsonObject.put("type", "BIGINT");
                return jsonObject;
            case "float":
                jsonObject.put("type", "FLOAT");
                jsonObject.put("precision", 6);
                return jsonObject;
            case "double":
                jsonObject.put("type", "DOUBLE");
                return jsonObject;
            case "char":
                jsonObject.put("type", "CHAR"); 
                jsonObject.put("length", -1); 
                return jsonObject;
            default:
                logger.error("Unrecognised conversion type: " + type);
                return null;
        }
    }
}
