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

package com.blobcity.db.operations;

import com.blobcity.db.export.ExportType;
import com.blobcity.db.importer.ImportType;
import static com.blobcity.db.operations.OperationTypes.IMPORT;
import org.springframework.stereotype.Component;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

/**
 *
 * @author sanketsarang
 */
@Component
public class OperationFactory {

    @Autowired
    @Qualifier("IndexOperation")
    private Operable indexOperation;
    @Autowired
    @Qualifier(value = "CsvImporter")
    private Operable csvImporter;
    @Autowired
    @Qualifier(value = "CsvExporter")
    private Operable csvExporter;

    public Operable getOperable(final OperationTypes operationType, final String... params) {
        switch (operationType) {
            case IMPORT:
                if (params.length == 0) {
                    return null;
                }

                final ImportType importType = ImportType.valueOf(params[0]);
                return getImportOperation(importType);
            case INDEX:
                return indexOperation;
            case EXPORT:
                if (params.length == 0) {
                    return null;
                }

                final ExportType exportType = ExportType.valueOf(params[0]);
                return getExportOperation(exportType);
        }

        return null;
    }

    private Operable getImportOperation(final ImportType importType) {
        switch (importType) {
            case CSV:
                return csvImporter;
        }

        return null;
    }

    private Operable getExportOperation(final ExportType exportType) {
        switch (exportType) {
            case CSV:
                return csvExporter;
        }

        return null;
    }
}
