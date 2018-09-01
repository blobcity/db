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

package com.blobcity.db.ftp;

import com.blobcity.db.exceptions.ErrorCode;
import com.blobcity.db.exceptions.OperationException;
import com.blobcity.util.security.PassGen;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.expression.Operation;
import org.springframework.stereotype.Component;

/**
 * @author sanketsarang
 */
@Component
public class FtpServiceManager {

    @Autowired
    private FtpServerManager ftpServerManager;

    public String startFtpService(final String ds) throws OperationException {
        final String password = PassGen.generate(20);
        ftpServerManager.setUser(ds, password);
        return password;
    }

    public void stopFtpService(final String ds) throws OperationException {
        ftpServerManager.removeUser(ds);
    }

    public String getFtpPassword(final String ds) throws OperationException {
        throw new OperationException(ErrorCode.OPERATION_NOT_SUPPORTED);
    }

    public boolean isEnabled(final String ds) throws OperationException {
        throw new OperationException(ErrorCode.OPERATION_NOT_SUPPORTED);
    }
}
