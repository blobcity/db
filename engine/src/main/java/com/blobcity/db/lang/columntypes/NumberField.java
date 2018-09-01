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

package com.blobcity.db.lang.columntypes;

import com.blobcity.db.exceptions.ErrorCode;
import com.blobcity.db.exceptions.OperationException;
import com.blobcity.db.schema.Types;
import com.blobcity.db.util.PreciseNumber;
import java.math.BigDecimal;
import org.json.JSONObject;

/**
 * <p>
 * Represents a field storing numeric data. The valid {@link Types} for use with this class are
 * <code>NUMERIC, DECIMAL, DEC, SMALLINT, INTEGER, INT, BIGINT, FLOAT, REAL, DOUBLE PRECISION</code>.
 *
 * <p>
 * The rule sets for the class initialization to succeed in terms of the optional values and precision / scale
 * specifications can be found at {@link http://docs.blobcity.com/display/DB/Data+Types}
 *
 * <p>
 * The class can be initialized by specifying a default value for the field which must be used in case the calling
 * function provides no value. The default value must comply with the set constraints of precision and scale.
 *
 * @author sanketsarang
 */
public final class NumberField implements FieldType<Number> {

    private final Types type;
    private final int precision;
    private final Integer scale;
    private final Number defaultValue;
    private final boolean hasDefaultValue;

    /**
     * Creates an instance of this class without a precision, scale or default value associated.
     *
     * @param type the {@link Types} of the numeric type this instance refers to
     * @throws OperationException if {@link Types} is not a numeric type or if the specified {@link Types} must have
     * either a precision or scale as a mandatory parameter
     */
    public NumberField(final Types type) throws OperationException {
        enforceNumberField(type);
        this.type = type;
        this.precision = getDefaultPrecision(type);
        this.scale = getDefaultScale(type, false);
        this.defaultValue = null;
        this.hasDefaultValue = false;
    }

    /**
     * Creates an instance of this class without a precision and scale specification but with a specified default value
     * for the field
     *
     * @param type the {@link Types} of the numeric type this instance refers to
     * @param defaultValue the default number value of the field
     * @throws OperationException if {@link Types} is not a numeric type or if the specified {@link Types} must have
     * either a precision or scale as a mandatory parameter, or if the specified default value violates the constraints
     * of the respective type
     */
    public NumberField(final Types type, final Number defaultValue) throws OperationException {
        enforceNumberField(type);
        this.type = type;
        this.precision = getDefaultPrecision(type);
        this.scale = getDefaultScale(type, false);
        this.defaultValue = convert(defaultValue);
        this.hasDefaultValue = true;
    }

    /**
     * Creates an instance of this class with a precision only specification.
     *
     * @param type the {@link Types} of the numeric type this instance refers to
     * @param precision the precision specification for the field
     * @throws OperationException if {@link Types} is not a numeric type or if the specified {@link Types} cannot have a
     * precision value
     */
    public NumberField(final Types type, final int precision) throws OperationException {
        enforceNumberField(type);
        validateOnlyPrecisionSpecification(type);
        validatePrecisionLimits(precision, type);
        this.type = type;
        if(precision == 0 ) this.precision = getDefaultPrecision(type);
        else this.precision = precision;
        this.scale = getDefaultScale(type, true);
        this.defaultValue = null;
        this.hasDefaultValue = false;
    }

    /**
     * Creates an instance of this class with scale only specification and precision defaulted to 38.
     *
     * @param scale the scale specification of the field
     * @param type the {@link Types} of the numeric type this instance refers to
     * @throws OperationException if {@link Types} is not a numeric type or if the specified {@link Types} cannot have a
     * scale value
     */
    public NumberField(final int scale, final Types type) throws OperationException {
        enforceNumberField(type);
        validatePrecisionAndScaleSpecification(type);
        validateScaleLimits(scale);
        this.type = type;
        this.precision = getDefaultPrecision(type);
        this.scale = scale;
        this.defaultValue = null;
        this.hasDefaultValue = false;
    }

    /**
     * Creates an instance of this class with a precision only specification and an associated default value
     *
     * @param type the {@link Types} of the numeric type this instance refers to
     * @param precision the precision specification for the field
     * @param defaultValue the default number value of the field
     * @throws OperationException if {@link Types} is not a numeric type or if the specified {@link Types} cannot have a
     * precision value or has a mandatory scale specification, or if the specified default value violates the
     * constraints of the respective type
     */
    public NumberField(final Types type, final int precision, final Number defaultValue) throws OperationException {
        enforceNumberField(type);
        validateOnlyPrecisionSpecification(type);
        validatePrecisionLimits(precision, type);
        this.type = type;
        this.precision = precision;
        this.scale = getDefaultScale(type, true);
        this.defaultValue = convert(defaultValue);
        this.hasDefaultValue = true;
    }

    /**
     * Creates an instance of this class with scale only specification
     *
     * @param scale the scale specification of the field
     * @param type the {@link Types} of the numeric type this instance refers to
     * @param defaultValue the default number value of the field
     * @throws OperationException if {@link Types} is not a numeric type or if the specified {@link Types} cannot have a
     * scale value, or if the specified default value violates the constraints of the respective type
     */
    public NumberField(final int scale, final Types type, final Number defaultValue) throws OperationException {
        enforceNumberField(type);
        validateOnlyPrecisionSpecification(type);
        validateScaleLimits(scale);
        this.type = type;
        this.precision = getDefaultPrecision(type);
        this.scale = scale;
        this.defaultValue = convert(defaultValue);
        this.hasDefaultValue = true;
    }

    /**
     * Creates an instance of this class with a precision and scale specification
     *
     * @param type the {@link Types} of the numeric type this instance refers to
     * @param precision the precision specification for the field
     * @param scale the scale specification for the field
     * @throws OperationException if {@link Types} is not a numeric type or if the specified {@link Types} cannot have
     * either a precision or scale specification
     */
    public NumberField(final Types type, final int precision, final int scale) throws OperationException {
        enforceNumberField(type);
        validatePrecisionAndScaleSpecification(type);
        validatePrecisionLimits(precision, type);
        validateScaleLimits(scale);
        if (scale > precision) {
            throw new OperationException(ErrorCode.DATATYPE_ERROR, "Scale must be less than or equal to precision");
        }
        this.type = type;
        this.precision = precision;
        this.scale = scale;
        this.defaultValue = null;
        this.hasDefaultValue = false;
    }

    /**
     * Creates an instance of this class with a precision and scale specification and an associated default value
     *
     * @param type the {@link Types} of the numeric type this instance refers to
     * @param precision the precision specification for the field
     * @param scale the scale specification for the field
     * @param defaultValue the default number value of the field
     * @throws OperationException if {@link Types} is not a numeric type or if the specified {@link Types} cannot have
     * either a precision or scale specification, or if the specified default value violates the constraints of the
     * respective type
     */
    public NumberField(final Types type, final int precision, final int scale, final Number defaultValue) throws OperationException {
        enforceNumberField(type);
        validatePrecisionAndScaleSpecification(type);
        validatePrecisionLimits(precision, type);
        validateScaleLimits(scale);
        if (scale > precision) {
            throw new OperationException(ErrorCode.DATATYPE_ERROR, "Scale must be less than or equal to precision");
        }
        this.type = type;
        this.precision = precision;
        this.scale = scale;
        this.defaultValue = convert(defaultValue);
        this.hasDefaultValue = true;
    }

    /**
     * Gets the {@link Types} associated with this field. Expected to be a numeric type.
     *
     * @return the {@link Types} associated with the field
     */
    @Override
    public Types getType() {
        return type;
    }

    /**
     * Getter for precision specification of the field
     *
     * @return the specified precision value of the field; -1 by default
     */
    public int getPrecision() {
        return precision;
    }

    /**
     * Getter for scale specification of the field
     *
     * @return the specified scale value of the field; -1 by default
     */
    public Integer getScale() {
        return scale;
    }

    /**
     * Getter for the default value specified for the field
     *
     * @return the default value specified for the field; <code>null</code> by default
     */
    @Override
    public Number getDefaultValue() {
        return defaultValue;
    }

    /**
     * Function to check if the field has an explicitely specified default value associated with it.
     *
     * @return <code>true</code> if default value was specified during object construction; <code>false</code> otherwise
     */
    @Override
    public boolean hasDefaultValue() {
        return hasDefaultValue;
    }

    /**
     * Converts the passed object to a {@link Number} type if conversion is possible. The input will be processed in
     * {@link String} form by converting the {@link Object} to {@link String} by using the <code>toString()</code>. Any
     * object that has a <code>toString()</code> function that results in a valid number expressed in {@link String}
     * form is a legal argument to this function.
     *
     * @param objValue an object representing a number, which has a <code>toString</code> method implementation that
     * produces a legal number.
     * @return an instance of {@link Number} created from the passed <code>objValue</code> with precision and scaling
     * constraints applied
     * @throws OperationException thrown with {@link ErrorCode} <code>DATATYPE_MISMATCH</code>
     */
    @Override
    public Number convert(Object objValue) throws OperationException {

        if (objValue == null) {
            throw new OperationException(ErrorCode.DATATYPE_MISMATCH);
        }

        switch (type) {
            case SMALLINT:
                try {
                    return Double.valueOf(objValue.toString()).shortValue();
                } catch (IllegalArgumentException ex) {
                    throw new OperationException(ErrorCode.INVALID_NUMBER_FORMAT, objValue.toString() + " could not be converted to a valid SMALLINT");
                }
            case INTEGER:
            case INT:
                try {
                    return Double.valueOf(objValue.toString()).intValue();
                } catch (IllegalArgumentException ex) {
                    throw new OperationException(ErrorCode.INVALID_NUMBER_FORMAT, objValue.toString() + " could not be converted to a valid INT/INTEGER");
                }
            case LONG:
            case BIGINT:
                try {
                    return Double.valueOf(objValue.toString()).longValue();
                } catch (IllegalArgumentException ex) {
                    throw new OperationException(ErrorCode.INVALID_NUMBER_FORMAT, objValue.toString() + " could not be converted to a valid BIGINT");
                }
            case DOUBLE:
            case DOUBLE_PRECISION:
                try {
                    return Double.valueOf(objValue.toString());
                } catch (IllegalArgumentException ex) {
                    throw new OperationException(ErrorCode.INVALID_NUMBER_FORMAT, objValue.toString() + " could not be converted to a valid DOUBLE/DOUBLE_PRECISION");
                }
            case REAL:
            case FLOAT:
                try {
                    return new PreciseNumber(new BigDecimal(objValue.toString()), precision).get();
                } catch (NumberFormatException ex) {
                    throw new OperationException(ErrorCode.INVALID_NUMBER_FORMAT, objValue.toString() + " could not be converted to a valid FLOAT / REAL");
                }
            case NUMERIC:
            case DECIMAL:
            case DEC:
                try {
                    if (scale == null) {
                        return new PreciseNumber(new BigDecimal(objValue.toString()), precision).get();
                    }

                    return new PreciseNumber(new BigDecimal(objValue.toString()), precision, scale).get();
                } catch (NumberFormatException ex) {
                    throw new OperationException(ErrorCode.INVALID_NUMBER_FORMAT, objValue.toString() + " could not be converted to a valid NUMERIC/DECIMAL/DEC");
                }
            default:
                throw new OperationException(ErrorCode.DATATYPE_ERROR, "The type " + type.getType() + " could not be identified as any valid numeric type");
        }
    }

    /**
     * Converts the field to {@link JSONObject} with all properties of the field included. The json will have the
     * following keys:
     * <ul>
     * <li><code>type</code>: always present
     * <li><code>precision</code>: if the field can have a precision parameter
     * <li><code>scale</code>: if the field can have a scale parameter
     * <li><code>default</code>: if the field has a default value associated
     * </ul>
     *
     * @return an instance of {@link JSONObject} representing the {@link NumberField}
     */
    @Override
    public JSONObject toJson() {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("type", getType().name());
        switch (getType()) {
            case DEC:
            case DECIMAL:
            case NUMERIC:
                jsonObject.put("scale", getScale()); //must flow over to the FLOAT case
            case FLOAT:
                jsonObject.put("precision", getPrecision());
        }
        if (hasDefaultValue()) {
            jsonObject.put("default", getDefaultValue());
        }
        return jsonObject;
    }

    /**
     * Throws an {@link OperationException} if the specified {@link Types} is not a valid numeric type
     *
     * @param type the {@link Types} to be checked to be a numeric type
     * @throws OperationException throw with {@link ErrorCode}
     * <code>DATATYPE_MISMATCH<code> if the specified {@link Types} is not a valid numeric type
     */
    private void enforceNumberField(Types type) throws OperationException {
        switch (type) {
            case NUMERIC:
            case DECIMAL:
            case DEC:
            case SMALLINT:
            case INTEGER:
            case INT:
            case LONG:
            case BIGINT:
            case FLOAT:
            case REAL:
            case DOUBLE:
            case DOUBLE_PRECISION:
                break;
            default:
                throw new OperationException(ErrorCode.DATATYPE_MISMATCH, type.getType() + " is not a valid Numeric field type");
        }
    }

    /**
     * Throws an exception if the specified {@link Types} requires a scale value specification or cannot have a
     * precision value specification
     *
     * @param type the {@link Types} to check for precision only specification validation
     * @throws OperationException thrown with {@link ErrorCode} <code>DATATYPE_CONSTRAINT_VIOLATION</code> if the
     * specified {@link Types} cannot have a precision specification or has a scale specification missing
     */
    private void validateOnlyPrecisionSpecification(Types type) throws OperationException {
        switch (type) {
            case NUMERIC:
            case DEC:
            case DECIMAL:
            case FLOAT:
                break;
            default:
                throw new OperationException(ErrorCode.DATATYPE_ERROR, "Data type " + type.getType() + " cannot have a precision specification");
        }
    }

    /**
     * Throws an exception if the specified {@link Types} cannot have a precision or scale specification.
     *
     * @param type the {@link Types} to check for both precision and scale as mandatory specifications
     * @throws OperationException thrown with {@link ErrorCode} <code>DATATYPE_CONSTRAINT_VIOLATION</code> if the
     * specified {@link Types} cannot have a precision or scale specification
     */
    private void validatePrecisionAndScaleSpecification(Types type) throws OperationException {
        switch (type) {
            case NUMERIC:
            case DEC:
            case DECIMAL:
                break;
            case FLOAT:
                throw new OperationException(ErrorCode.DATATYPE_ERROR, "Data type " + type.getType() + " can have only a precision specification but attempted to set a scale specification as well");
            default:
                throw new OperationException(ErrorCode.DATATYPE_ERROR, "Date type " + type.getType() + " must not have either precision or scale specified");
        }
    }

    /**
     * Return the default precision value associated with the specified type if a default is possible. Returns -1 for
     * field types that do not have a precision specification.
     *
     * @param type the {@link Types} for which the default precision value is to be obtained
     * @return the default precision value associated with the specified {@link Types}; -1 by default
     */
    private int getDefaultPrecision(Types type) {
        switch (type) {
            case NUMERIC:
            case DEC:
            case DECIMAL:
                return 38;
            case REAL:
            case FLOAT:
                return 24;
            default:
                return -1;
        }
    }

    /**
     * Gets the default scale to be set for the specified numeric {@link Types} with an additional control of whether
     * precision is specified for the number or not. {@link Types.NUMERIC}, {@link Types.DEC} and {@link Types.DECIMAL}
     * require the scale to be set to 0 if the number has a precision specified, else there is no scale specification.
     *
     * @param type the {@link Types} who default scale value is sought
     * @param precisionSpecified {@link Boolean} parameter indicating whether number has a precision specified or not
     * @return the default scale value to be used
     */
    private Integer getDefaultScale(Types type, boolean precisionSpecified) {
        switch (type) {
            case NUMERIC:
            case DEC:
            case DECIMAL:
                if (precisionSpecified) {
                    return 0;
                }
                return null;
            default:
                return null;
        }
    }

    /**
     * Will throw an exception if scale value passed is less than -84 or greater than 127.
     *
     * @param scale the scale value to validate
     * @throws OperationException if value of scale lies outside the permitted range
     */
    private void validateScaleLimits(int scale) throws OperationException {
        if (scale < -84 || scale > 127) {
            throw new OperationException(ErrorCode.DATATYPE_ERROR, "Scale must be in between -84 and 127");
        }
    }

    /**
     * Will throw an exception if precision value passed is less than or equal to 0 or greater than 127 for all number
     * types, and will validate precision to be a maximum of 24 for <code>FLOAT</code> type.
     *
     * @param precision the precision value to validate
     * @param type the {@link Types} of the type for which precision is being validated
     * @throws OperationException if value of precision lies outside the permitted range of the database, or permitted
     * range of the specified type
     */
    private void validatePrecisionLimits(int precision, Types type) throws OperationException {
        
        if (type == Types.FLOAT && precision > 24) {
            throw new OperationException(ErrorCode.DATATYPE_ERROR, "Maximum precision permitted for FLOAT type is 24");
        }
        
        if (precision <= 0 || precision > 127) {
            throw new OperationException(ErrorCode.DATATYPE_ERROR, "Precision must be greater than 0 and less than or equal to 127");
        }        
    }
}
