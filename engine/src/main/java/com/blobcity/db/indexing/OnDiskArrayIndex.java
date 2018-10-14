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
import com.blobcity.db.exceptions.ErrorCode;
import com.blobcity.db.exceptions.OperationException;

import java.util.Iterator;
import java.util.Set;

/**
 * @author sanketsarang
 *
 * TODO: Implement this
 */
public class OnDiskArrayIndex implements IndexingStrategy {

    @Override
    public void index(String app, String table, String column, String columnValue, String pk) throws OperationException {

    }

    @Override
    public Set<String> loadIndex(String app, String table, String column, String columnValue) throws OperationException {
        return null;
    }

    @Override
    public Set<String> loadIndex(String app, String table, String column, OperatorFileFilter filter) throws OperationException {
        return null;
    }

    @Override
    public Iterator<String> loadIndexStream(String app, String table, String column, String columnValue) throws OperationException {
        return null;
    }

    @Override
    public Iterator<String> loadIndexStream(String app, String table, String column, OperatorFileFilter filter) throws OperationException {
        return null;
    }

    @Override
    public void initializeIndexing(String app, String table, String column) throws OperationException {

    }

    @Override
    public boolean contains(String app, String table, String column, String columnValue, String pk) throws OperationException {
        return false;
    }

    @Override
    public void remove(String app, String table, String column, String columnValue, String pk) throws OperationException {

    }

    @Override
    public void dropIndex(String app, String table, String column) throws OperationException {

    }

    @Override
    public Iterator<String> cardinality(String app, String table, String column) throws OperationException {
        return null;
    }

    @Override
    public long readIndexCount(String app, String table, String column, String columnValue) throws OperationException {
        return 0;
    }

    @Override
    public void writeIndexCount(String app, String table, String column, String columnValue, long count) throws OperationException {

    }

    @Override
    public String getAnyCardinalEntry(final String ds, final String collection, final String column, final String columnValue) throws OperationException {
        throw new OperationException(ErrorCode.OPERATION_NOT_SUPPORTED, "Fetching single cardinal entry on OnDisk Array index is not supported");
    }

    @Override
    public long getIndexCount(String app, String table, String column, String columnValue) throws OperationException {
        throw new OperationException(ErrorCode.OPERATION_NOT_SUPPORTED, "Requested operation not supported with index type of column: " + column);
    }
}
