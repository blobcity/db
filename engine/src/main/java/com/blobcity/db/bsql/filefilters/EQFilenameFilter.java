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
 * Used to get files by processing files names that match the reference value in the desired operation. This class satisfies all files that satisfy equals
 * operator with reference value.
 *
 * @author sanketsarang
 * @param <T>
 */
public class EQFilenameFilter<T extends Comparable<T>> implements OperatorFileFilter {

    private final TypeConverter<T> typeConverter;
    private final T referenceValue;

    public EQFilenameFilter(TypeConverter<T> typeConverter, T referenceValue) {
        this.typeConverter = typeConverter;
        this.referenceValue = referenceValue;
    }

    @Override
    public boolean accept(Object entry) throws IOException {
        try {
            return referenceValue.equals(typeConverter.getValue(getColumnValue((Path) entry)));
        } catch (OperationException ex) {
            LoggerFactory.getLogger(EQFilenameFilter.class.getName()).error(null, ex);
            throw new IOException();
        }
    }

    private String getColumnValue(Path path) throws OperationException {
        return FileNameEncoding.decode(path.getFileName().toString());
    }

    @Override
    public Object getReferenceValue() {
        return this.referenceValue;
    }

    @Override
    public TypeConverter getTypeConverter() {
        return this.typeConverter;
    }

    @Override
    public Object getTypeConvertedReferenceValue() throws OperationException {
        if(referenceValue instanceof Boolean) {
            if(((Boolean) referenceValue).booleanValue()) return "true";
            else return "false";
        }
        return typeConverter.getValue(referenceValue.toString());
    }
}
