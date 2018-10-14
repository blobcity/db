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

package com.blobcity.db.indexing;

import com.blobcity.db.bsql.filefilters.EQFilenameFilter;
import com.blobcity.db.bsql.filefilters.OperatorFileFilter;
import com.blobcity.db.exceptions.DbRuntimeException;
import com.blobcity.db.exceptions.ErrorCode;
import com.blobcity.db.exceptions.OperationException;
import com.blobcity.db.sql.util.PathUtil;
import com.blobcity.db.util.FileNameEncoding;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.HashSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 *
 * @author sanketsarang
 */
@Component
public class OnDiskUniqueIndex implements IndexingStrategy {

    @Override
    public void index(String app, String table, String column, String columnValue, String pk) throws OperationException {
        /* Ignore indexing of null or empty values */
        if(columnValue == null || columnValue.isEmpty()) {
            return;
        }

        Path path = Paths.get(PathUtil.indexColumnValueFolder(app, table, column, columnValue));
        if (!Files.exists(path)) {
            try {
                Files.write(path, pk.getBytes());
            } catch (IOException ex) {

                //TODO: Notify admin
                LoggerFactory.getLogger(OnDiskBTreeIndex.class.getName()).error(null, ex);
                throw new OperationException(ErrorCode.INDEXING_ERROR, "The index for column: " + column + " in table: "
                        + table + " could not be created for column value: " + columnValue
                        + " mapping to a record with primary key: " + pk);
            }
        }
    }

    @Override
    public Set<String> loadIndex(String app, String table, String column, String columnValue) throws OperationException {
        Set<String> set = new HashSet<>();
        Path path = Paths.get(PathUtil.indexColumnValueFolder(app, table, column, columnValue));
        if (Files.exists(path)) {
            try {
                set.addAll(Files.readAllLines(path, Charset.defaultCharset()));
            } catch (IOException ex) {

                //TODO: Notify admin
                LoggerFactory.getLogger(OnDiskUniqueIndex.class.getName()).error(null, ex);
                throw new OperationException(ErrorCode.INDEXING_ERROR, "Unable to read index for column: " + column
                        + " in table: " + table + " while searching for indexed value: " + columnValue);
            }
        }
        return set;
    }

    /**
     * Loads pk's for all index values matching the specified filter criteria
     *
     * @param app the BlobCity application id
     * @param table the name of the table within the BlobCity application
     * @param column the name of the column within the specified table
     * @param filter a file filter to select only files that match the specified condition
     * @return <code>Set<String></code> containing all pk's that match the filter criteria; an empty set if no value
     * matches the search criteria
     * @throws OperationException if a files system error occurs when reading the index.
     */
    @Override
    public Set<String> loadIndex(String app, String table, String column, OperatorFileFilter filter) throws OperationException {
        throw new OperationException(ErrorCode.OPERATION_NOT_SUPPORTED);
    }

    @Override
    public Iterator<String> loadIndexStream(String app, String table, String column, String columnValue) throws OperationException {
        Iterator<String> iterator;
        Path path = Paths.get(PathUtil.indexColumnValueFolder(app, table, column, columnValue));
        if (Files.exists(path)) {
            try {
                final BufferedReader reader = Files.newBufferedReader(path, Charset.defaultCharset());
                iterator = new Iterator<String>() {
                    private String line = null;

                    @Override
                    public boolean hasNext() {
                        if (line == null) {
                            try {
                                line = reader.readLine();
                            } catch (IOException ex) {
                                return false;
                            }
                        }

                        if(line == null) {
                            try {
                                reader.close();
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }

                        return line != null;
                    }

                    @Override
                    public String next() {
                        if (line == null) {
                            try {
                                line = reader.readLine();
                                if (line == null) {
                                    throw new NoSuchElementException();
                                }
                            } catch (IOException ex) {
                                throw new NoSuchElementException("Either end of stream is reached for reading index, or an IO exception occured while reading unique index");
                            }
                        }

                        String innerLine = line;
                        line = null;
                        return innerLine;
                    }

                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException("Remove operation cannot be invoked when reading unique index as stream");
                    }
                };
                return iterator;
            } catch (IOException ex) {

                //TODO: Notify admin
                LoggerFactory.getLogger(OnDiskUniqueIndex.class.getName()).error(null, ex);
                throw new OperationException(ErrorCode.INDEXING_ERROR, "Unable to read index for column: " + column
                        + " in table: " + table + " while searching for indexed value: " + columnValue);
            }
        }

        return null;
    }

    /**
     * Loads an iterator over pk's for all index values matching the specified filter criteria
     *
     * @param app the BlobCity application id
     * @param table the name of the table within the BlobCity application
     * @param column the name of the column within the specified table
     * @param filter a file filter to select only files that match the specified condition
     * @return <code>Iterator<String></code> containing all pk's that match the filter criteria; an empty iterator if no
     * value matches the search criteria
     * @throws OperationException if a files system error occurs when reading the index.
     */
    @Override
    public Iterator<String> loadIndexStream(String app, String table, String column, OperatorFileFilter filter) throws OperationException {
        Path path = Paths.get(PathUtil.indexColumnFolder(app, table, column));
        Iterator<String> iterator;
        
        if(filter instanceof EQFilenameFilter) {
            return loadIndexStream(app, table, column, filter.getTypeConvertedReferenceValue().toString());
        }
        
        try {
            DirectoryStream<Path> fileStream = Files.newDirectoryStream(path, filter);
            iterator = new Iterator<String>() {
                Iterator<Path> fileIterator = fileStream.iterator();

                @Override
                public boolean hasNext() {
                    boolean hasNext = fileIterator.hasNext();
                    if(!hasNext) {
                        try {
                            fileStream.close();
                        } catch (IOException ex) {
                            Logger.getLogger(OnDiskUniqueIndex.class.getName()).log(Level.SEVERE, null, ex);
                        }
                    }
                    return fileIterator.hasNext();
                }

                @Override
                public String next() {
                    if (!fileIterator.hasNext()) {
                        try {
                            fileStream.close();
                        } catch (IOException ex) {
                            Logger.getLogger(OnDiskUniqueIndex.class.getName()).log(Level.SEVERE, null, ex);
                        }
                        throw new NoSuchElementException();
                    }
                    String fileName = fileIterator.next().getFileName().toString();
                    try {
                        return FileNameEncoding.decode(fileName);
                    } catch (OperationException ex) {
                        LoggerFactory.getLogger(OnDiskBTreeIndex.class.getName()).error("Failed to decode string: " + fileName, ex);
                        throw new DbRuntimeException(ex);
                    }
                }

                @Override
                public void remove() {
                    fileIterator.remove();
                }
            };
        } catch (IOException ex) {
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR);
        }

        return iterator;
    }

    @Override
    public void initializeIndexing(String app, String table, String column) throws OperationException {
        Path path = Paths.get(PathUtil.indexColumnFolder(app, table, column));
        if (!Files.exists(path)) {
            try {
                Files.createDirectories(path);
            } catch (IOException ex) {

                //TODO: Notify admin
                LoggerFactory.getLogger(OnDiskBTreeIndex.class.getName()).error(null, ex);
                throw new OperationException(ErrorCode.INDEXING_ERROR, "The index for column: " + column + " in table: "
                        + table + " could not be created");
            }
        }
    }

    @Override
    public boolean contains(String app, String table, String column, String columnValue, String pk) throws OperationException {
        Path path = Paths.get(PathUtil.indexColumnValueFolder(app, table, column, columnValue));
        return Files.exists(path);
    }

    @Override
    public void remove(String app, String table, String column, String columnValue, String pk) throws OperationException {
        Path path = Paths.get(PathUtil.indexColumnValueFolder(app, table, column, columnValue));
        if (!Files.exists(path)) {
            return;
        }

        try {
            String actualPk = new String(Files.readAllBytes(path));

            if (actualPk.equals(pk)) {
                Files.delete(path);
            } else {
                throw new OperationException(ErrorCode.INDEXING_ERROR, "Incorrect primary key specification while "
                        + "deleting Unique index record of column: " + column + " with column value: " + columnValue
                        + " in table: " + table + ". The primary key on record is: " + actualPk + " but attempting "
                        + "remove for primary key: " + pk);
            }
        } catch (IOException ex) {

            //TODO: Notify admin
            LoggerFactory.getLogger(OnDiskBTreeIndex.class.getName()).error(null, ex);
            throw new OperationException(ErrorCode.INDEXING_ERROR, "The index for column: " + column + " in table: "
                    + table + " could not be created for column value: " + columnValue
                    + " mapping to a record with primary key: " + pk);
        }
    }

    @Override
    public void dropIndex(String app, String table, String column) throws OperationException {
        Path sourcePath = Paths.get(PathUtil.indexColumnFolder(app, table, column));
        Path destinationPath = Paths.get(PathUtil.globalDeleteFolder(app + "_" + table + "_" + column + "_" + System.currentTimeMillis()));
        try {
            Files.move(sourcePath, destinationPath, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException ex) {

            //TODO: Notify admin
            LoggerFactory.getLogger(OnDiskBTreeIndex.class.getName()).error(null, ex);
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "Could not drop index for column: "
                    + column + " in table: " + table);
        }
    }

    @Override
    public Iterator<String> cardinality(String app, String table, String column) throws OperationException {
        return null;
    }

    /**
     * Gets the primary key associated with the specified unique index file
     *
     * @param app the BlobCity application id of the application
     * @param table name of table within the BlobCity application
     * @param column name of column within the table name
     * @param columnValue the value of the specified column who's mapping pk is to be loaded
     * @return the pk mapping to the unique index for the specified column value; null if no index entry found for the
     * specified column value
     * @throws OperationException if an I/O error occurs or if the entry is corrupted
     */
    private String getPk(String app, String table, String column, String columnValue) throws OperationException {
        throw new OperationException(ErrorCode.OPERATION_NOT_SUPPORTED);
    }

    /**
     * Will return either -1 or 1 by checking for existing of the index entry and not the index count file. Unique index
     * types does not have any index count file entry.
     *
     * @param app the application id of the BlobCity application
     * @param table name of table within the application
     * @param column name of column within the table
     * @param columnValue value of the column for which the index entry isPresent
     * @return 1 if corresponding index entry is found; -1 otherwise
     * @throws OperationException never thrown
     */
    @Override
    public long readIndexCount(String app, String table, String column, String columnValue) throws OperationException {
        Path path = Paths.get(PathUtil.indexColumnValueFolder(app, table, column, columnValue));
        if (Files.exists(path)) {
            return 1;
        } else {
            return -1;
        }
    }

    /**
     * This function is always a no-op
     *
     * @param app the application id of the BlobCity application
     * @param table name of table within the application
     * @param column name of column within the table
     * @param columnValue value of the column for which the index entry isPresent
     * @param count the revised size of the specified index cardinal. value is ignored.
     * @throws OperationException never thrown
     */
    @Override
    public void writeIndexCount(String app, String table, String column, String columnValue, long count) throws OperationException {
        //do nothing as unique index can have possible size outcomes of only 1 or -1 in the read operation
    }

    @Override
    public String getAnyCardinalEntry(final String ds, final String collection, final String column, final String columnValue) throws OperationException {
        throw new OperationException(ErrorCode.OPERATION_NOT_SUPPORTED, "Fetching single cardinal entry on OnDisk Unique index is not supported");
    }

    @Override
    public long getIndexCount(String app, String table, String column, String columnValue) throws OperationException {
        throw new OperationException(ErrorCode.OPERATION_NOT_SUPPORTED, "Requested operation not supported with index type of column: " + column);
    }
}
