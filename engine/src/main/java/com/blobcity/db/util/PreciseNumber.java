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

package com.blobcity.db.util;

import com.blobcity.db.exceptions.ErrorCode;
import com.blobcity.db.exceptions.OperationException;
import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * <p>
 * Represents a number with scale and precision being optional specifications, that operates exactly as per SQL
 * specifications for constraints and approximations in storing NUMERIC type values. The default precision is 38 and
 * scale is defaulted to the scale of the value set, hence all values after the decimal point are retained by default
 * unless a scale value is explicitely specified.
 *
 * <p>
 * <b>This class does not ensure thread safety or function synchronization. However one instance will be ensured to
 * operation without any side effects on other instances or the program in general. If a single object is shared across
 * threads, the client program needs to ensure thread safety</b>
 *
 * @author sanketsarang
 */
public class PreciseNumber {

    private final BigDecimal bigDecimal;
    private final int precision;
    private final int scale;

    public PreciseNumber(BigDecimal bigDecimal) throws OperationException {
        this(bigDecimal, 38, bigDecimal.scale());
    }

    public PreciseNumber(int scale, BigDecimal bigDecimal) throws OperationException {
        this(bigDecimal, 38, scale);
    }

    public PreciseNumber(BigDecimal bigDecimal, int precision) throws OperationException {
        this(bigDecimal, precision, bigDecimal.scale());
    }

    public PreciseNumber(BigDecimal bigDecimal, int precision, int scale) throws OperationException {
        /* Check precision to be a positive number */
        if (precision <= 0) {
            throw new OperationException(ErrorCode.DATATYPE_ERROR, "Precision must be greater than zero");
        }
        
        if(scale > precision) {
            throw new OperationException(ErrorCode.DATATYPE_ERROR, "Scale must be less than or equal to precision");
        }
        
        this.precision = precision;
        this.scale = scale;
        this.bigDecimal = applyPrecisionAndScale(bigDecimal);
    }

    public BigDecimal getBigDecimal() {
        return bigDecimal;
    }

    public int getPrecision() {
        return precision;
    }

    public int getScale() {
        return scale;
    }

    public BigDecimal get() {
        return this.bigDecimal;
    }

    /**
     * Processes the BigDecimal in accordance with the precision and scale constraint and returns a modified BigDecimal
     * which abides by the constraint. If the number cannot be approximated to abide by the constraint an
     * {@link OperationException} is thrown
     *
     * @param bigDecimal the {@link BigDecimal} that is to be checked and modified to abide by the precision and scale
     * constraints
     * @return modified {@link BigDecimal} that abides by the specified precision and scale constraint
     * @throws OperationException if the precision constraint cannot be satisfied
     */
    private BigDecimal applyPrecisionAndScale(BigDecimal bigDecimal) throws OperationException {
        BigDecimal bd = bigDecimal.setScale(scale, RoundingMode.HALF_UP);
        final String bdString = bd.stripTrailingZeros().abs().toPlainString();
        final int bdPrecision;
        if (bdString.contains(".")) {
            bdPrecision = bdString.length() - 1;
        } else {
            bdPrecision = bdString.length();
        }
        if (bdPrecision > precision) {
            throw new OperationException(ErrorCode.DATATYPE_CONSTRAINT_VIOLATION, "Found number with precision "
                    + bdPrecision + " but maximum permitted precision is " + precision);
        }
        return bd;
    }
}
