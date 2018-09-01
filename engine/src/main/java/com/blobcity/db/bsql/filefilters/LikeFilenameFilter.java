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

package com.blobcity.db.bsql.filefilters;

import com.blobcity.db.exceptions.OperationException;
import com.blobcity.db.lang.datatypes.converters.TypeConverter;
import com.blobcity.db.util.FileNameEncoding;
import java.io.IOException;
import java.nio.file.Path;
import org.slf4j.LoggerFactory;

/**
 * Used to get files by processing files names that match the reference value in the desired operation. This class
 * satisfies all files that satisfy like operator with reference value.
 *
 * <b>This class is currently implemented to check if the search string passed in constructor is present within the
 * specific values. It does a 'contains' check instead of 'like' check.</b>
 *
 * @author sanketsarang
 */
public class LikeFilenameFilter implements OperatorFileFilter {

    private final String searchString;

    public LikeFilenameFilter(String searchString) {
        this.searchString = searchString;
    }

    @Override
    public boolean accept(Object entry) throws IOException {
        try {
            //TODO: Make rule set for the like operation for processing wildcard characters in the searchString
            return getColumnValue((Path) entry).contains(searchString);
        } catch (OperationException ex) {
            LoggerFactory.getLogger(LikeFilenameFilter.class.getName()).error(null, ex);
            throw new IOException();
        }
    }

    private String getColumnValue(Path path) throws OperationException {
        return FileNameEncoding.decode(path.getFileName().toString());
    }

    @Override
    public Object getReferenceValue() {
        return this.searchString;
    }

    @Override
    public TypeConverter getTypeConverter() {
        throw new UnsupportedOperationException("Operation not supported. Invalid invocation case for getTypeConverter() on LIKE operator");
    }

    @Override
    public Object getTypeConvertedReferenceValue() throws OperationException {
        return this.searchString;
    }
}
