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
import com.blobcity.db.bsql.filefilters.InFilenameFilter;
import com.blobcity.db.bsql.filefilters.OperatorFileFilter;
import com.blobcity.db.exceptions.DbRuntimeException;
import com.blobcity.db.exceptions.ErrorCode;
import com.blobcity.db.exceptions.OperationException;
import com.blobcity.db.indexcache.OnDiskBtreeIndexCache;
import com.blobcity.db.features.FeatureRules;
import com.blobcity.db.sql.util.PathUtil;
import com.blobcity.db.util.FileNameEncoding;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.DirectoryStream;
import java.nio.file.DirectoryStream.Filter;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.blobcity.util.lambda.Counter;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

/**
 *
 * @author sanketsarang
 */
@Component
public class OnDiskBTreeIndex implements IndexingStrategy {

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(OnDiskBTreeIndex.class.getName());

    @Autowired
    private IndexCountStore indexCountStore;
    @Autowired @Lazy
    private OnDiskBtreeIndexCache indexCache;

    /**
     * <p>
     * Adds an index entry for the specified column for the specified record. </p>
     *
     * <p>
     * The function will also invoke an increment operation on {@link IndexCountStore} if the indexing operation
     * succeeds. Calling this function on an already indexed may result in an incorrect count within
     * {@link IndexCountStore}</p>
     *
     *
     * @param app the application id of the BlobCity application
     * @param table name of table within the application
     * @param column name of column within the table
     * @param columnValue value of the column or the cardinal value for which the index is to be created
     * @param pk the primary key value of the record who's column value is provided for indexing
     * @throws OperationException if a file system error occurs in writing the index entry
     */
    @Override
    public void index(String app, String table, String column, String columnValue, String pk) throws OperationException {
        /* Ignore indexing of null or empty values */
        if(columnValue == null || columnValue.isEmpty()) {
            return;
        }

        /* Create column value folder if it does not already exist */
        Path path = Paths.get(PathUtil.indexColumnValueFolder(app, table, column, columnValue));
        //TODO: This operation is possibly not thread safe
        if (!Files.exists(path)) {
            try {
                Files.createDirectory(path);
            } catch (IOException ex) {

                //TODO: Notify admin
                LoggerFactory.getLogger(OnDiskBTreeIndex.class.getName()).error(null, ex);
                throw new OperationException(ErrorCode.INDEXING_ERROR, "The index for column: " + column + " in table: "
                        + table + " could not be created");
            }
        }

        /* Create pk file inside column value folder */
        path = Paths.get(PathUtil.indexColumnValueFolder(app, table, column, columnValue) + pk);
        if (!Files.exists(path)) {
            try {
                Files.createFile(path);
            } catch (IOException ex) {

                //TODO: Notify admin
                LoggerFactory.getLogger(OnDiskBTreeIndex.class.getName()).error(null, ex);
                throw new OperationException(ErrorCode.INDEXING_ERROR, "The index for column: " + column + " in table: "
                        + table + " could not be created for column value: " + columnValue
                        + " mapping to a record with primary key: " + pk);
            }
        }

        if(FeatureRules.INDEX_CACHING) {
            indexCache.addEntry(app, table, column, columnValue, pk);
        }

        indexCountStore.incrementCount(app, table, column, columnValue, this);
    }

    @Override
    public Set<String> loadIndex(String app, String table, String column, String columnValue) throws OperationException {
        if(FeatureRules.INDEX_CACHING) {
            final Set<String> cachedIndex = indexCache.get(app, table, column, columnValue);
            if (cachedIndex != null) {
                return cachedIndex;
            }
        }

        final Set<String> set = new HashSet<>();
        Path path = Paths.get(PathUtil.indexColumnValueFolder(app, table, column, columnValue));
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(path)) {
            Iterator<Path> iterator = stream.iterator();
            while (iterator.hasNext()) {
                String pk = iterator.next().getFileName().toString();
                set.add(FileNameEncoding.decode(pk));
            }
        } catch (IOException ex) {

            //TODO: Notify admin
            LoggerFactory.getLogger(OnDiskBTreeIndex.class.getName()).error(null, ex);
            throw new OperationException(ErrorCode.INDEXING_ERROR, "Could not read index for column: " + column
                    + "  in table: " + table + " for searched value: " + columnValue);
        }

        if(FeatureRules.INDEX_CACHING) {
            indexCache.cache(app, table, column, columnValue, set);
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
        Set<String> set = new HashSet<>();
        Iterator<String> iterator = loadIndexStream(app, table, column, filter);

        while (iterator.hasNext()) {
            String next = iterator.next();
            set.add(FileNameEncoding.decode(next));
        }

        return set;
    }

    @Override
    public Iterator<String> loadIndexStream(String app, String table, String column, String columnValue) throws OperationException {
        if(FeatureRules.INDEX_CACHING) {
            final Set<String> cachedIndex = indexCache.get(app, table, column, columnValue);
            if (cachedIndex != null) {
                return cachedIndex.iterator();
            }
        }

        final Path path = Paths.get(PathUtil.indexColumnValueFolder(app, table, column, columnValue));
        if(!Files.exists(path)) {
            return Collections.emptyIterator();
        }

        Iterator<String> iterator;
        try {
            DirectoryStream<Path> directoryStream = Files.newDirectoryStream(path);
            iterator = new Iterator<String>() {
                Iterator<Path> innerIterator = directoryStream.iterator();

                @Override
                public boolean hasNext() {
                    boolean hasNext = innerIterator.hasNext();
                    if (!hasNext) {
                        try {
                            directoryStream.close();
                        } catch (IOException ex) {
                            Logger.getLogger(OnDiskBTreeIndex.class.getName()).log(Level.SEVERE, null, ex);
                        }
                    }
                    return hasNext;
                }

                @Override
                public String next() {
                    String fileName = innerIterator.next().getFileName().toString();
                    try {
                        return FileNameEncoding.decode(fileName);
                    } catch (OperationException ex) {
                        LoggerFactory.getLogger(OnDiskBTreeIndex.class.getName()).error("Unable to decode string: " + fileName, ex);
                        throw new DbRuntimeException(ex);
                    }
                }

                @Override
                public void remove() {
                    innerIterator.remove();
                }
            };
        } catch (IOException ex) {
            logger.error("Error reading index" , ex);
            return null;
        }

        /* In case caching is enabled, will execute the full iterator, cache and return an iterator from cache */
        if(FeatureRules.INDEX_CACHING) {
            Set<String> pkSet = new HashSet<>();
            iterator.forEachRemaining(pk -> pkSet.add(pk));
            indexCache.cache(app, table, column, columnValue, pkSet);
            return pkSet.iterator();
        } else {
            return iterator;
        }
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
    public Iterator<String> loadIndexStream(final String app, final String table, final String column, OperatorFileFilter filter) throws OperationException {
        Path path = Paths.get(PathUtil.indexColumnFolder(app, table, column));

        if (filter instanceof EQFilenameFilter) {
            return loadIndexStream(app, table, column, filter.getTypeConvertedReferenceValue().toString());
        } else if (filter instanceof InFilenameFilter) {
            return inOperatorIterator(app, table, column, (Set<String>) filter.getReferenceValue());
        } else {
            return genericFilterIterator(path, filter);
        }
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
        if(FeatureRules.INDEX_CACHING) {
            Optional<Boolean> optional = indexCache.contains(app, table, column, columnValue, pk);
            if(optional.isPresent()) {
                return optional.get();
            }
        }

        Path path = Paths.get(PathUtil.indexColumnValueFolder(app, table, column, columnValue) + pk);
        return Files.exists(path);
    }

    /**
     * <p>
     * Removes an existing entry within the specified column, for the specified column value for the specified record.
     * The operation is a no-op if the specified entry is not found.</p>
     *
     * <p>
     * The function will invoke a decrement on {@link IndexCountStore} if an index entry was actually removed by this
     * operation</p>
     *
     * @param app the application id of the BlobCity application
     * @param table name of table within the application
     * @param column name of column within the table
     * @param columnValue value of the column for which the entry is to be removed. Also the cardinal identifying the
     * index entry.
     * @param pk the primary key of the record which current holds an entry under the specified cardinal
     * @throws OperationException if a file system error occurs while removing the index entry.
     */
    @Override
    public void remove(String app, String table, String column, String columnValue, String pk) throws OperationException {
        Path path = Paths.get(PathUtil.indexColumnValueFolder(app, table, column, columnValue) + pk);
        try {
            if (Files.deleteIfExists(path)) {
                indexCountStore.decrementCount(app, table, column, columnValue, this);
                if(FeatureRules.INDEX_CACHING) {
                    indexCache.removeEntry(app, table, column, columnValue, pk);
                }
            }
        } catch (IOException ex) {

            //TODO: Notify admin
            LoggerFactory.getLogger(OnDiskBTreeIndex.class.getName()).error(null, ex);
            throw new OperationException(ErrorCode.INDEXING_ERROR, "Unable to remove indexed value for column: "
                    + column + " in table: " + table + " when attempting to deleted index value: " + columnValue);
        }
    }

    @Override
    public void dropIndex(String app, String table, String column) throws OperationException {
        Path sourcePath = Paths.get(PathUtil.indexColumnFolder(app, table, column));
        Path destinationPath = Paths.get(PathUtil.globalDeleteFolder(app + "_" + table + "_" + column + "_" + System.currentTimeMillis()));
        try {
            Files.move(sourcePath, destinationPath, StandardCopyOption.ATOMIC_MOVE);
            if(FeatureRules.INDEX_CACHING) {
                indexCache.invalidate(app, table, column);
            }
        } catch (IOException ex) {

            //TODO: Notify admin
            LoggerFactory.getLogger(OnDiskBTreeIndex.class.getName()).error(null, ex);
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "Could not drop index for column: "
                    + column + " in table: " + table);
        }
    }

    @Override
    public Iterator<String> cardinality(String app, String table, String column) throws OperationException {
        if(FeatureRules.INDEX_CACHING) {
            Optional<Set<String>> cardinalSet = indexCache.cardinality(app, table, column);
            if(cardinalSet.isPresent()) {
                return cardinalSet.get().iterator();
            }
        }

        Path path = Paths.get(PathUtil.indexColumnFolder(app, table, column));
        Iterator<String> iterator;
        try {
            DirectoryStream directoryStream = Files.newDirectoryStream(path);
            iterator = new Iterator<String>() {
                Iterator<Path> innerIterator = directoryStream.iterator();

                @Override
                public boolean hasNext() {
                    boolean hasNext = innerIterator.hasNext();
                    if (!hasNext) {
                        try {
                            directoryStream.close();
                        } catch (IOException ex) {
                            Logger.getLogger(OnDiskBTreeIndex.class.getName()).log(Level.SEVERE, null, ex);
                        }
                    }
                    return hasNext;
                }

                @Override
                public String next() {
                    String fileName = innerIterator.next().getFileName().toString();
                    try {
                        return FileNameEncoding.decode(fileName);
                    } catch (OperationException ex) {
                        LoggerFactory.getLogger(OnDiskBTreeIndex.class.getName()).error("Failed to decode string: " + fileName, ex);
                        throw new DbRuntimeException(ex);
                    }
                }

                @Override
                public void remove() {
                    innerIterator.remove();
                }
            };
        } catch (IOException ex) {
            return null;
        }

        return iterator;
    }

    /**
     * Reads the size of the cardinal for the specified index entry. The function call results in an I/O operation to
     * read the data from the respective index count file
     *
     * @param app the application id of the BlobCity application
     * @param table name of table within the application
     * @param column name of column within the table
     * @param columnValue value of the column for which the index entry isPresent
     * @return the size of the specified cardinal if entry is found; -1 otherwise
     * @throws OperationException if an I/O error occurs in reading an existent count file. Absence of file will return
     * a -1 and not result in an exception
     */
    @Override
    public long readIndexCount(String app, String table, String column, String columnValue) throws OperationException {
        Path path = FileSystems.getDefault().getPath(PathUtil.indexCountFile(app, table, column, columnValue));
        if (!Files.exists(path)) {
            return -1;
        }

        try {
            return Long.parseLong(Files.readAllLines(path, Charset.defaultCharset()).get(0));
        } catch (IOException ex) {
            LoggerFactory.getLogger(OnDiskBTreeIndex.class.getName()).error(null, ex);
            throw new OperationException(ErrorCode.INDEX_COUNT_ERROR, "Could not successfully read index count");
        } catch (NumberFormatException ex) {
            LoggerFactory.getLogger(OnDiskBTreeIndex.class.getName()).error(null, ex);
            throw new OperationException(ErrorCode.INDEX_COUNT_ERROR, "Index count data is corrupted");
        }
    }

    /**
     * Persists the count value representing the size of the specified index cardinal to the file system inside the
     * respective index count file. Passing a value of zero or lesser will result in the specified index count file
     * being deleted.
     *
     * @param app the application id of the BlobCity application
     * @param table name of table within the application
     * @param column name of column within the table
     * @param columnValue value of the column for which the index entry isPresent
     * @param count the revised size of the specified index cardinal
     * @throws OperationException if an I/O error occurs while writing to the index count file
     */
    @Override
    public void writeIndexCount(String app, String table, String column, String columnValue, long count) throws OperationException {
        Path path = FileSystems.getDefault().getPath(PathUtil.indexCountFile(app, table, column, columnValue));
        Path columnFolderPath;

        /* If count is less than or equal to zero means the count entry should no longer exist. Hence delete count file
         instead of updateding it */
        if (count <= 0) {
            try {
                Files.deleteIfExists(path);
                return;
            } catch (IOException ex) {
                LoggerFactory.getLogger(OnDiskBTreeIndex.class.getName()).error(null, ex);
                throw new OperationException(ErrorCode.INDEX_COUNT_ERROR, "Could not successfully delete index count file");
            }
        }

        try {
            if (!Files.exists(path)) {
                columnFolderPath = FileSystems.getDefault().getPath(PathUtil.indexCountColumnFolder(app, table, column));

                //TODO: Optimise so that the column directory is created from start and does not have to be checked for or done here
                Files.createDirectories(columnFolderPath);
            }

            Files.write(path, ("" + count).getBytes());
        } catch (IOException ex) {
            LoggerFactory.getLogger(OnDiskBTreeIndex.class.getName()).error(null, ex);
            throw new OperationException(ErrorCode.INDEX_COUNT_ERROR, "Could not successfully write index count");
        }
    }

    private Iterator<String> genericFilterIterator(Path path, Filter filter) throws OperationException {
        try {
            DirectoryStream<Path> folderStream = Files.newDirectoryStream(path, filter);
            Iterator<String> iterator = new Iterator<String>() {
                Iterator<Path> folderIterator = folderStream.iterator();
                Iterator<Path> fileIterator = null;
                DirectoryStream<Path> fileStream = null;

                @Override
                public boolean hasNext() {
                    if (fileIterator == null || !fileIterator.hasNext()) {
                        if (!attemptNextFolder()) {
                            try {
                                folderStream.close();
                            } catch (IOException ex) {
                                Logger.getLogger(OnDiskBTreeIndex.class.getName()).log(Level.SEVERE, null, ex);
                            }
                            return false;
                        }
                    }
                    return fileIterator.hasNext();
                }

                @Override
                public String next() {
                    if (fileIterator == null || !fileIterator.hasNext()) {
                        if (!attemptNextFolder()) {
                            throw new NoSuchElementException();
                        }
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
                    folderIterator.remove();
                }

                private boolean attemptNextFolder() {
                    if (folderIterator.hasNext()) {
                        if (fileStream != null) {
                            try {
                                fileStream.close();
                            } catch (IOException ex) {
                                Logger.getLogger(OnDiskBTreeIndex.class.getName()).log(Level.SEVERE, null, ex);
                            }
                        }
                        try {
                            fileStream = Files.newDirectoryStream(folderIterator.next());
                            fileIterator = fileStream.iterator();
                            return true;
                        } catch (IOException ex) {
                            LoggerFactory.getLogger(OnDiskBTreeIndex.class.getName()).error(null, ex);

                            //TODO: An unexpected situation. However a way of reporting this needs to be identified
                        }
                    } else {
                        if (fileStream != null) {
                            try {
                                fileStream.close();
                            } catch (IOException ex) {
                                Logger.getLogger(OnDiskBTreeIndex.class.getName()).log(Level.SEVERE, null, ex);
                            }
                        }
                    }
                    return false;
                }
            };

            return iterator;
        } catch (IOException ex) {
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR);
        }
    }

    private Iterator<String> inOperatorIterator(final String app, final String table, final String column, final Set<String> values) {

        if (values == null) {
            return Collections.EMPTY_LIST.iterator();
        }

        if(FeatureRules.INDEX_CACHING) {
            Optional<Set<String>> optional = indexCache.inQuery(app, table, column, values);
            if(optional.isPresent()) {
                return optional.get().iterator();
            }
        }

        Iterator<String> iterator = new Iterator<String>() {
            Iterator<String> currentIterator = null;
            List<String> searchValues = new ArrayList<>(values);
            int index = 0;

            @Override
            public boolean hasNext() {
                if (currentIterator == null) {
                    currentIterator = getNextInnerIterator(); //this function may return null as a valid case
                }

                if (currentIterator == null) {
                    return false;
                }
                
                if(!currentIterator.hasNext()) {
                    currentIterator = getNextInnerIterator();
                }
                
                if(currentIterator == null) {
                    return false;
                }
                
                return currentIterator.hasNext();
            }

            @Override
            public String next() {
                if (currentIterator == null) {
                    currentIterator = getNextInnerIterator(); //this function may return null as a valid case
                }

                if (currentIterator == null) {
                    throw new NoSuchElementException();
                }
                
                if(!currentIterator.hasNext()) {
                    currentIterator = getNextInnerIterator(); //this function may return null as a valid case
                }
                
                if(currentIterator == null) {
                    throw new NoSuchElementException();
                }
                
                return currentIterator.next();
            }

            private Iterator<String> getNextInnerIterator() {
                Iterator nextIterator;

                do {
                    if (index >= searchValues.size()) {
                        return null;
                    }
                    try {
                        nextIterator = loadIndexStream(app, table, column, searchValues.get(index++));
                    } catch (OperationException ex) {
                        Logger.getLogger(OnDiskBTreeIndex.class.getName()).log(Level.SEVERE, null, ex);
                        return null;
                    }
                } while (nextIterator == null || !nextIterator.hasNext());

                return nextIterator;
            }
        };

        return iterator;
    }

    public String getAnyCardinalEntry(final String ds, final String collection, final String column, final String columnValue) throws OperationException {
        if(FeatureRules.INDEX_CACHING) {
            final Set<String> pkSet = indexCache.get(ds, collection, column, columnValue);
            if(pkSet != null && !pkSet.isEmpty()) {
                return pkSet.iterator().next();
            }
        }

        Path path = Paths.get(PathUtil.indexColumnValueFolder(ds, collection, column, columnValue));
        DirectoryStream<Path> directoryStream = null;
        try {
            directoryStream = Files.newDirectoryStream(path);
        } catch (IOException e) {
            throw new OperationException(ErrorCode.INDEXING_ERROR, "Unable to read OnDisk BTree index");
        }
        Iterator<Path> iterator = directoryStream.iterator();
        if(iterator.hasNext()) {
            return FileNameEncoding.decode(iterator.next().getFileName().toString());
        } else {
            throw new OperationException(ErrorCode.SELECT_ERROR, "No entry found in supposedly valid cardinal inside OnDisk BTree index");
        }
    }


    @Override
    public long getIndexCount(String app, String table, String column, String columnValue) throws OperationException {
        if(FeatureRules.INDEX_CACHING) {
            final Set<String> cachedIndex = indexCache.get(app, table, column, columnValue);
            if (cachedIndex != null) {
                return (long) cachedIndex.size();
            }
        }

        final Path path = Paths.get(PathUtil.indexColumnValueFolder(app, table, column, columnValue));
        if(!Files.exists(path)) {
            return 0L;
        }

        try {
            DirectoryStream<Path> directoryStream = Files.newDirectoryStream(path);
            Counter counter = new Counter();
            directoryStream.iterator().forEachRemaining(p -> {
                counter.increment();
            });

            return counter.getCount();
        } catch (IOException ex) {
            //do nothing
        }

        throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "Error reading index");
    }
}
