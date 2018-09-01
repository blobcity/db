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
import java.util.Set;
import org.slf4j.LoggerFactory;

/**
 * Used to get files by processing files names that match the reference value in the desired operation. This class satisfies all files that satisfy IN operator
 * with reference values.
 *
 * @param <T> The java data type of elements that are to be checked with the filter condition. A type converter of the same type must be provided
 *
 * @author sanketsarang
 */
public class InFilenameFilter<T> implements OperatorFileFilter {

    private final TypeConverter<T> typeConverter;
    private final Set<Object> searchSet;

    public InFilenameFilter(TypeConverter typeConverter, Set<Object> searchSet) {
        this.typeConverter = typeConverter;
        this.searchSet = searchSet;
    }

    @Override
    public boolean accept(Object entry) throws IOException {
        try {
            return searchSet.contains(typeConverter.getValue(getColumnValue((Path) entry)));
        } catch (OperationException ex) {
            LoggerFactory.getLogger(InFilenameFilter.class.getName()).error(null, ex);
            throw new IOException();
        }
    }

    private String getColumnValue(Path path) throws OperationException {
        return FileNameEncoding.decode(path.getFileName().toString());
    }

    @Override
    public Object getReferenceValue() {
        return this.searchSet;
    }

    @Override
    public TypeConverter getTypeConverter() {
        return this.typeConverter;
    }

    @Override
    public Object getTypeConvertedReferenceValue() throws OperationException {
        return this.searchSet;
    }
}
