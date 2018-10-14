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

import com.blobcity.db.bsql.filefilters.OperatorFileFilter;
import com.blobcity.db.exceptions.OperationException;
import java.util.Iterator;
import java.util.Set;

/**
 *
 * @author sanketsarang
 */
public interface IndexingStrategy {

    public void index(String app, String table, String column, String columnValue, String pk) throws OperationException;

    /**
     * Loads all values present within the specified index
     *
     * @param app The application id of the BlobCity application
     * @param table The name of the table within the specified application
     * @param column The name of the column within the specified table
     * @param columnValue The value of the column for which the index is to be fetched
     * @return <code>Set<String></code> containing all primary keys that match the column value within the specified
     * indexed column
     * @throws OperationException if an error occurs performing the operation, or an I/O error occurs in reading the
     * index store
     */
    public Set<String> loadIndex(String app, String table, String column, String columnValue) throws OperationException;

    public Set<String> loadIndex(String app, String table, String column, OperatorFileFilter filter) throws OperationException;

    public Iterator<String> loadIndexStream(String app, String table, String column, String columnValue) throws OperationException;

    public Iterator<String> loadIndexStream(String app, String table, String column, OperatorFileFilter filter) throws OperationException;

    public long getIndexCount(String app, String table, String column, String columnValue) throws OperationException;

    /**
     * Applies an indexing on all records of the specified column only if no initializeIndexing currently is present for
     * this column. The indexing process is asynchronous so a tracking id for the indexing operation is returned for
     * checking status of an indexing operation.
     *
     * @param app
     * @param table
     * @param column
     * @return
     */
    public void initializeIndexing(String app, String table, String column) throws OperationException;

    /**
     * Checks whether an initializeIndexing column has an entry for the specified column value that satisfies the
     * specified primary key. In other words it checks if the record identified by the primary key has value of the
     * specified column same as column value specified.
     *
     * @param app
     * @param table
     * @param column
     * @param columnValue
     * @param pk
     * @return
     * @throws com.blobcity.db.exceptions.OperationException
     */
    public boolean contains(String app, String table, String column, String columnValue, String pk) throws OperationException;

    public void remove(String app, String table, String column, String columnValue, String pk) throws OperationException;

    public void dropIndex(String app, String table, String column) throws OperationException;

    public Iterator<String> cardinality(String app, String table, String column) throws OperationException;

    /**
     * Reads the size of the cardinal for the specified index entry. The function call results in an I/O operation to
     * read the data from the respective index count file
     *
     * @param app the application id of the BlobCity application
     * @param table name of table within the application
     * @param column name of column within the table
     * @param columnValue value of the column for which the index entry is present
     * @return the size of the specified cardinal if entry is found; -1 otherwise
     * @throws OperationException if an I/O error occurs in reading an existent count file. Absence of file will return
     * a -1 and not result in an exception
     */
    public long readIndexCount(String app, String table, String column, String columnValue) throws OperationException;

    /**
     * Persists the count value representing the size of the specified index cardinal to the file system inside the
     * respective index count file. Passing a value of zero or lesser will result in the specified index count file
     * being deleted.
     *
     * @param app the application id of the BlobCity application
     * @param table name of table within the application
     * @param column name of column within the table
     * @param columnValue value of the column for which the index entry is present
     * @param count the revised size of the specified index cardinal
     * @throws OperationException if an I/O error occurs while writing to the index count file
     */
    public void writeIndexCount(String app, String table, String column, String columnValue, long count) throws OperationException;

    /**
     * Gets a single record key from within the specified cardinal if the cardinal exists
     * @param ds name of datastore
     * @param collection name of collection
     * @param column name of column
     * @param columnValue value of column in string form - this is the cardinal inside which to look
     * @return _id of any record present within the cardinal
     * @throws OperationException if the cardinal cannot be found, or any other error occurs
     */
    public String getAnyCardinalEntry(final String ds, final String collection, final String column, final String columnValue) throws OperationException;
}
