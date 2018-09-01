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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author sanketsarang
 */
public class BetweenFilenameFilter<T extends Comparable<T>> implements OperatorFileFilter {

    private static final Logger logger = LoggerFactory.getLogger(BetweenFilenameFilter.class.getName());

    private final TypeConverter<T> typeConverter;
    private final T referenceValue1;
    private final T referenceValue2;

    public BetweenFilenameFilter(TypeConverter<T> typeConverter, T referenceValue1, T referenceValue2) {
        this.typeConverter = typeConverter;
        int compared = 0;
        try {
            compared = typeConverter.getValue(referenceValue1.toString()).compareTo(typeConverter.getValue(referenceValue2.toString()));
        } catch (OperationException ex) {
            logger.error("BETWEEN " + referenceValue1.toString() + " AND " + referenceValue2 + " failed initialisation", ex);
        }
        if (compared > 0) {
            this.referenceValue1 = referenceValue2;
            this.referenceValue2 = referenceValue1;
        } else {
            this.referenceValue1 = referenceValue1;
            this.referenceValue2 = referenceValue2;
        }
    }

    @Override
    public boolean accept(Object entry) throws IOException {
        try {
            String value = getColumnValue((Path) entry);
            return referenceValue1.compareTo(typeConverter.getValue(value)) >= 0 && referenceValue2.compareTo(typeConverter.getValue(value)) <= 0;
        } catch (OperationException ex) {
            LoggerFactory.getLogger(GTFilenameFilter.class.getName()).error(null, ex);
            throw new IOException();
        }
    }

    private String getColumnValue(Path path) throws OperationException {
        return FileNameEncoding.decode(path.getFileName().toString());
    }

    @Override
    public Object getReferenceValue() {
        throw new UnsupportedOperationException("Operation not supported. Invalid invocation case for getReferenceValue() on BETWEEN operator");
    }

    @Override
    public TypeConverter getTypeConverter() {
        return this.typeConverter;
    }

    @Override
    public Object getTypeConvertedReferenceValue() throws OperationException {
        throw new UnsupportedOperationException("Operation not supported. Invalid invocation case for getTypeConvertedReferenceValue() on BETWEEN operator");
    }
}
