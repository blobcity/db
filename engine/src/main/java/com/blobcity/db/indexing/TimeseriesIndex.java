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
 *
 * @author sanketsarang
 */
public class TimeseriesIndex implements IndexingStrategy {

    @Override
    public void index(String app, String table, String column, String columnValue, String pk) throws OperationException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Set<String> loadIndex(String app, String table, String column, String columnValue) throws OperationException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Set<String> loadIndex(String app, String table, String column, OperatorFileFilter filter) throws OperationException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Iterator<String> loadIndexStream(String app, String table, String column, String columnValue) throws OperationException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Iterator<String> loadIndexStream(String app, String table, String column, OperatorFileFilter filter) throws OperationException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void initializeIndexing(String app, String table, String column) throws OperationException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public boolean contains(String app, String table, String column, String columnValue, String pk) throws OperationException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void remove(String app, String table, String column, String columnValue, String pk) throws OperationException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void dropIndex(String app, String table, String column) throws OperationException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Iterator<String> cardinality(String app, String table, String column) throws OperationException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public long readIndexCount(String app, String table, String column, String columnValue) throws OperationException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void writeIndexCount(String app, String table, String column, String columnValue, long count) throws OperationException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public String getAnyCardinalEntry(final String ds, final String collection, final String column, final String columnValue) throws OperationException {
        throw new OperationException(ErrorCode.OPERATION_NOT_SUPPORTED, "Fetching single cardinal entry on OnDisk Timeseries index is not supported");
    }

    @Override
    public long getIndexCount(String app, String table, String column, String columnValue) throws OperationException {
        throw new OperationException(ErrorCode.OPERATION_NOT_SUPPORTED, "Requested operation not supported with index type of column: " + column);
    }
}
