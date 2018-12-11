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
import com.blobcity.db.exceptions.OperationException;
import com.blobcity.db.sp.DataExporter;
import com.blobcity.db.sp.export.ExcelExport;
import com.blobcity.lib.export.ExportType;
import com.blobcity.lib.export.GenericExportResponse;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * @author sanketsarang
 */
public class ExportServiceRouterBean implements ExportServiceRouter{

    @Autowired
    private ExportProcedureStore exportProcedureStore;

    @Override
    public GenericExportResponse export(final String ds, final String spName, final ExportType exportType, final String paramString) {
        DataExporter dataExporter = null;
        try {
            dataExporter = exportProcedureStore.newInstance(ds, spName);
        } catch (OperationException e) {
            return new GenericExportResponse("error.txt", new ByteArrayInputStream(("Error occurred loading exporter in: " + ds + " with name: " + spName + " and export type: " + exportType).getBytes()));
        }

        switch(exportType) {
            case CSV:
                break;
            case EXCEL:
                ExcelExport excelExport = dataExporter.getAsExcel(paramString);
                ByteArrayOutputStream stream = new ByteArrayOutputStream();
                try {
                    excelExport.getWorkbook().write(stream);
                    return new GenericExportResponse(excelExport.getFilename(), new ByteArrayInputStream(stream.toByteArray()));
                } catch (IOException e) {
                    e.printStackTrace();
                    return new GenericExportResponse("error.txt", new ByteArrayInputStream("Error occurred".getBytes()));
                }
            case PDF:
                break;
            case TEXT:
                break;
            case JSON:
                break;
            case XML:
                break;
        }

        return new GenericExportResponse("error.txt", new ByteArrayInputStream("Nothing to export".getBytes()));
    }
}
