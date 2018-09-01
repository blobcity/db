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

package com.blobcity.db.code.procedures;
import com.blobcity.db.exceptions.ErrorCode;
import com.blobcity.db.exceptions.OperationException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * This class is used to execute various namedProcedure in given procedureStore
 * 
 * @author sanketsarang
 */
@Component
public class ProcedureExecutorBean {
    
    private static final Logger logger = LoggerFactory.getLogger(ProcedureExecutorBean.class);
    
    @Autowired
    private ProcedureStoreBean procedureStore;
    
    
    public Object executeProcedure(final String requestId, final String app, final String procedureName, Object[] parameters) throws OperationException {
        Method method = procedureStore.getNamedProcedure(app, procedureName);
        procedureStore.getClass(app, procedureName);
        final String procedureStoreName = procedureName.substring(0, procedureName.indexOf("."));
        Object instance = procedureStore.getNewInstance(requestId, app, procedureStoreName);

        if (instance == null) {
            throw new OperationException(ErrorCode.STORED_PROCEDURE_NOT_LOADED, "Could not find stored procedure "
                    + procedureName + " inside app " + app);
        }

        if (parameters != null) {

            //TODO: This is a valid code and is put only for sake of custom error reporting. This can be eventually removed to improve performance.
            Class[] paramClasses = method.getParameterTypes();
            if (paramClasses.length != parameters.length) {
                throw new OperationException(ErrorCode.STORED_PROCEDURE_INCORRECT_PARAMS, "Required " + paramClasses.length
                        + " parameters but found " + parameters.length + " parameters");
            }

            /* Validate all parameters to be of the correct type */
            for (int i = 0; i < paramClasses.length; i++) {
                Class paramClass = paramClasses[i];
                try {
                    paramClass.cast(parameters[i]);
                } catch (ClassCastException ex) {
                    throw new OperationException(ErrorCode.STORED_PROCEDURE_INCORRECT_PARAMS, "Could not parse parameter " + parameters[i].toString() + " to the required type " + paramClass.getCanonicalName());
                }
            }
        }

        return executeMethod(method, instance, parameters);
    }

    private Object executeMethod(Method method, Object instance, Object[] parameters) throws OperationException {
        try {
            if (parameters == null) {
                return method.invoke(instance);
            } else {
                return method.invoke(instance, parameters);
            }
        } catch (IllegalAccessException ex) {
            logger.error("Required procedure method does not have public access", ex);
            throw new OperationException(ErrorCode.STORED_PROCEDURE_EXECUTION_ERROR, "Required procedure method does not have public access");
        } catch (IllegalArgumentException ex) {
            logger.error(null, ex);
            throw new OperationException(ErrorCode.STORED_PROCEDURE_INCORRECT_PARAMS, "Illegal arguments passed to named procedure");
        } catch (InvocationTargetException ex) {
            logger.error("Execution of named procedure failed", ex);
            throw new OperationException(ErrorCode.STORED_PROCEDURE_EXECUTION_ERROR, "Execution of named procedure failed");
        }
    }
}
