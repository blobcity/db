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

package com.blobcity.db.export;

import com.blobcity.code.ExportServiceRouter;
import com.blobcity.db.sp.export.ExcelExport;
import com.blobcity.lib.export.ExportType;
import com.blobcity.lib.export.GenericExportResponse;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * @author sanketsarang
 */
public class ExportServiceRouterBean implements ExportServiceRouter{

    @Override
    public GenericExportResponse export(String spName, ExportType exportType) {
        switch(exportType) {
            case CSV:
            case EXCEL:
                ExcelExport excelExport = new ExcelExport();
                ByteArrayOutputStream stream = new ByteArrayOutputStream();
                try {
                    excelExport.getWorkbook().write(stream);
                    return new GenericExportResponse(excelExport.getFilename(), new String(stream.toByteArray()));
                } catch (IOException e) {
                    e.printStackTrace();
                    return new GenericExportResponse("error.txt", e.getMessage());
                }
            case PDF:
            case TEXT:
            case JSON:
            case XML:
        }

        return new GenericExportResponse("error.txt", "Nothing to export");
    }
}
