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

package com.blobcity.db.storage;

import com.blobcity.db.cache.CacheRules;
import com.blobcity.db.cache.DataCache;
import com.blobcity.db.exceptions.DbRuntimeException;
import com.blobcity.db.exceptions.ErrorCode;
import com.blobcity.db.exceptions.OperationException;
import com.blobcity.db.features.FeatureRules;
import com.blobcity.db.locks.LockType;
import com.blobcity.db.locks.TransactionLocking;
import com.blobcity.db.sql.util.PathUtil;
import com.blobcity.db.util.FileNameEncoding;
import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.DirectoryStream.Filter;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;

import com.google.common.collect.Iterators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * This class is responsible for reading and writing files. This class operates on files in the .bdb format at a low
 * level to directly modify the contents of the file.<br/><br/> This class supports thread safe and transacted
 * operations.
 *
 * @author sanketsarang
 */
@Component
public class BSqlFileManager {

    private static final Logger logger = LoggerFactory.getLogger(BSqlFileManager.class.getName());

    @Autowired
    private DataCache dataCache;
    @Autowired
    private CacheRules cacheRules;
    @Autowired
    private TransactionLocking transactionLocking;

    /**
     * <p>
     * Selects the record matching the key from the predefined table. This function currently identifies only the file
     * name as a valid key. The implementation must be changed to allow fetching from any indexed field of the table</p>
     *
     * @param app the application id of the BlobCity application
     * @param table the table within the specified application
     * @param key the key of the record to select
     * @return selected record
     * @throws com.blobcity.db.exceptions.OperationException
     */
    public String select(final String app, final String table, final String key) throws OperationException {
        String result = null;
        if (app.equals(".systemdb") || (FeatureRules.DATA_CACHING && cacheRules.shouldCache(app, table))) {
            result = dataCache.load(app, table, key);
            if (result != null) {
                return result;
            }
        }
        transactionLocking.acquireLock(app, table, key, LockType.READ);
        try {
            Path path = Paths.get(PathUtil.dataFile(app, table, key));
            try {
                result = new String(Files.readAllBytes(path), "UTF-8");
                if (FeatureRules.DATA_CACHING && cacheRules.shouldCache(app, table)) {
                    dataCache.cache(app, table, key, result);
                }
                return result;
            } catch (IOException e) {
                throw new OperationException(ErrorCode.PRIMARY_KEY_INEXISTENT, "A record with the given primary key: " + key + " could not be found in table: " + table);
            }
        } finally {
            transactionLocking.releaseLock(app, table, key, LockType.READ);
        }

        /* Old and stable implementation. Was not working for special characters */
//        try (BufferedReader reader = new BufferedReader(new FileReader(PathUtil.dataFile(app, table, key)))) {
//            result = reader.readLine();
//
//            if (LicenseRules.DATA_CACHING && cacheRules.shouldCache(app, table)) {
//                dataCache.cache(app, table, key, result);
//            }
//            return result;
//        } catch (FileNotFoundException ex) {
//            throw new OperationException(ErrorCode.PRIMARY_KEY_INEXISTENT, "A record with the given primary key: " + key + " could not be found in table: " + table);
//        } catch (IOException ex) {
//            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "An internal operation error occured. Could not select record for table: " + table);
//        }
    }

    public List<String> selectAll(final String app, final String table) throws OperationException {
        List<String> list = new ArrayList<>();
        try (DirectoryStream directoryStream = Files.newDirectoryStream(FileSystems.getDefault().getPath(PathUtil.dataFolderPath(app, table)))) {
            Iterator<Path> iterator = directoryStream.iterator();

            //TODO: Manually iterator and apply governor limit if required
            iterator.forEachRemaining(path -> {
                String fileName = path.getFileName().toString();
                try {
                    list.add(FileNameEncoding.decode(fileName));
                } catch (OperationException ex) {
                    logger.error(null, ex);
                }
            });

            return list;
        } catch (IOException ex) {
            logger.trace(null, ex);
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR);
        }
    }

    public int rowCount(final String ds, final String collection) throws OperationException {
        try (DirectoryStream directoryStream = Files.newDirectoryStream(FileSystems.getDefault().getPath(PathUtil.dataFolderPath(ds, collection)))) {
            return Iterators.size(directoryStream.iterator());
        } catch (IOException ex) {
            logger.trace(null, ex);
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR);
        }
    }

    /**
     * Gets all primary keys within the specified table in the form of an iterator. Only one key is loaded at any point
     * in this. No select governor limits apply on this function.
     *
     * @param app The application id of the application
     * @param table The table name of the table who's records are to be selected
     * @return An <code>Iterator<String></code> which iterates over primary keys of all records in the table.
     * @throws IOException If an i/o error occurs
     */
    public Iterator<String> selectAllKeysAsStream(final String app, final String table) throws IOException {
        try(DirectoryStream ds = Files.newDirectoryStream(FileSystems.getDefault().getPath(PathUtil.dataFolderPath(app, table)))) {
            final Iterator<Path> pathIterator = ds.iterator();

            return new Iterator<String>() {
                @Override
                public boolean hasNext() {
                    boolean hasNext = pathIterator.hasNext();
                    if (!hasNext) {
                        try {
                            ds.close();
                        } catch (IOException ex) {
                            java.util.logging.Logger.getLogger(BSqlFileManager.class.getName()).log(Level.SEVERE, null, ex);
                        }
                    }
                    return hasNext;
                }

                @Override
                public String next() {
                    String fileName = pathIterator.next().getFileName().toString();
                    try {
                        return FileNameEncoding.decode(fileName);
                    } catch (OperationException ex) {
                        logger.error("Decoding failed for string: " + fileName, ex);
                        throw new DbRuntimeException(ex);
                    }
                }

                @Override
                public void remove() {
                    pathIterator.remove();
                }
            };
        }
    }

    /**
     * Gets an iterator over primary keys for all key records that match the filter condition. The filter condition
     * should ideally apply only on values of the primary keys and not values of other columns, for which functions
     * within {@link BSqlIndexManager} should be used instead
     *
     * @param app The application id of the BLobCity application
     * @param table name of table within the BlobCity application
     * @param filter the filter that needs to be satisfied for the returned values
     * @return A <code>Iterator<String></code> containing primary keys that match the filter criteria
     * @throws IOException if an exception occurs in reading data from the file system. Will indicate lack of file
     * system read permissions on non existence of required data storage or corrupt data.
     */
    public Iterator<String> selectWithFilterAsStream(final String app, final String table, final Filter filter) throws IOException {

        try(DirectoryStream ds = Files.newDirectoryStream(FileSystems.getDefault().getPath(PathUtil.dataFolderPath(app, table)), filter)) {
            final Iterator<Path> pathIterator = ds.iterator();
            return new Iterator<String>() {
                @Override
                public boolean hasNext() {
                    boolean hasNext = pathIterator.hasNext();
                    if (!hasNext) {
                        try {
                            ds.close();
                        } catch (IOException ex) {
                            java.util.logging.Logger.getLogger(BSqlFileManager.class.getName()).log(Level.SEVERE, null, ex);
                        }
                    }
                    return hasNext;
                }

                @Override
                public String next() {
                    String name = pathIterator.next().getFileName().toString();
                    try {
                        return FileNameEncoding.decode(name);
                    } catch (OperationException ex) {
                        logger.error("Failed to decode String: " + name, ex);
                        throw new DbRuntimeException(ex);
                    }
                }

                @Override
                public void remove() {
                    pathIterator.remove();
                }
            };
        }
    }

    /**
     * Gets a set of primary keys for all key records that match the filter condition. The filter condition should
     * ideally apply only on values of the primary keys and not values of other columns, for which functions within
     * {@link BSqlIndexManager} should be used instead. This function internally using the streaming (iterator)
     * equivalent.
     *
     * @param app The application id of the BLobCity application
     * @param table name of table within the BlobCity application
     * @param filter the filter that needs to be satisfied for the returned values
     * @return A <code>Set<String></code> containing primary keys that match the filter criteria
     * @throws IOException if an exception occurs in reading data from the file system. Will indicate lack of file
     * system read permissions on non existence of required data storage or corrupt data.
     * @throws com.blobcity.db.exceptions.OperationException For internal decoding error
     */
    public Set<String> selectWithFilter(final String app, final String table, final Filter filter) throws IOException, OperationException {
        Set<String> set = new HashSet<>();
        Iterator<String> iterator = selectWithFilterAsStream(app, table, filter);
        while (iterator.hasNext()) {
            String next = iterator.next();
            set.add(FileNameEncoding.decode(next));
        }
        return set;
    }

    /**
     * Checks whether an item with the given key is present or not
     *
     * @param app The application id of the BlobCity application
     * @param table The name of the table within the application
     * @param key The key to search
     * @return true if a row with the key is found, false otherwise
     * @throws com.blobcity.db.exceptions.OperationException for internal decoding error
     */
    public boolean exists(final String app, final String table, final String key) throws OperationException {
        return new File(PathUtil.dataFile(app, table, key)).exists();
    }

    /**
     * Deletes a given row in the database. The function will return the number of rows that are deleted by this
     * execution.
     *
     * @param app The application id of the BlobCity application
     * @param table The name of the table within the application
     * @param key The row mapped to the key to delete
     */
    public void remove(final String app, final String table, String key) throws OperationException {
        transactionLocking.acquireLock(app, table, key, LockType.WRITE);
        try {
            Path path = Paths.get(PathUtil.dataFile(app, table, key));
            if (!Files.exists(path)) {
                throw new OperationException(ErrorCode.PRIMARY_KEY_INEXISTENT, "A record with the given primary key: " + key + " could not be found in table: " + table);
            }
            try {
                if (Files.deleteIfExists(path) && FeatureRules.DATA_CACHING) {
                    dataCache.invalidate(app, table, key);
                }
            } catch (IOException ex) {

                //TODO: Notify admin
                throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "An internal operation error occured. Could not delete record in table: " + table);
            }
        } finally {
            transactionLocking.releaseLock(app, table, key, LockType.WRITE);
        }
    }

    /**
     * DOCUMENT ME
     *
     * @param app
     * @param table
     * @param key
     * @param jsonString
     * @throws com.blobcity.db.exceptions.OperationException
     */
    public void save(final String app, final String table, final String key, final String jsonString) throws OperationException {
        transactionLocking.acquireLock(app, table, key, LockType.WRITE);
        try {
            Path path = Paths.get(PathUtil.dataFile(app, table, key));
            try {
                Files.write(path, jsonString.getBytes("UTF-8"));
                if (app.equals(".systemdb") || (FeatureRules.DATA_CACHING && cacheRules.shouldCache(app, table))) {
                    dataCache.cache(app, table, key, jsonString);
                }
            } catch (IOException ex) {

                //TODO: Notify admin
                throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "An internal operation error occured. Could not commit save operation to file system for table: " + table);
            }
        } finally {
            transactionLocking.releaseLock(app, table, key, LockType.WRITE);
        }
    }

    public void insert(final String app, final String table, final String key, final String jsonString) throws OperationException {
        transactionLocking.acquireLock(app, table, key, LockType.WRITE);
        try {
            Path path = Paths.get(PathUtil.dataFile(app, table, key));
            if (Files.exists(path)) {
                throw new OperationException(ErrorCode.PRIMARY_KEY_CONFLICT, "A record with the given primary key: " + key + " already isPresent in table: " + table);
            }
            try {
                Files.write(path, jsonString.getBytes("UTF-8"));
                if (app.equals(".systemdb") || (FeatureRules.DATA_CACHING && FeatureRules.CACHE_INSERTS && cacheRules.shouldCache(app, table))) {
                    dataCache.cache(app, table, key, jsonString);
                }
            } catch (IOException ex) {
                //TODO: Notify admin
                throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "An internal operation error occured. Could not commit insert operation to file system for table: " + table);
            }
        } finally {
          transactionLocking.releaseLock(app, table, key, LockType.WRITE);
        }
    }

    public boolean rename(final String app, final String table, final String existingKey, final String newKey) throws OperationException {
        try {
            File file = new File(PathUtil.dataFile(app, table, existingKey));
            File newFile = new File(PathUtil.dataFile(app, table, newKey));

            /* file should be present in order to proceed */
            if (!file.exists()) {
                return false;
            }

            /* perform the rename operation */
            if (file.renameTo(newFile)) {
                
                /* Update cache */
                if (app.equals(".systemdb") || (FeatureRules.DATA_CACHING && cacheRules.shouldCache(app, table))) {
                    String cachedValue = dataCache.load(app, table, existingKey);
                    dataCache.invalidate(app, table, existingKey);
                    dataCache.cache(app, table, newKey, cachedValue);
                }
                return true;
            }

        } catch (Exception ex) {
            logger.error("Rename failed: " + existingKey + " to " + newKey + " failed for appId:" + app + ", table:" + table, ex);
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "An internal operation error occurred. Failed to rename file");
        }

        return false;
    }
}
