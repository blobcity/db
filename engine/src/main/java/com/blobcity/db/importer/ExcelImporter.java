package com.blobcity.db.importer;

import com.blobcity.db.exceptions.ErrorCode;
import com.blobcity.db.exceptions.OperationException;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.springframework.stereotype.Component;
import org.tensorflow.Operation;

import java.io.File;
import java.io.IOException;

@Component
public class ExcelImporter {

    public void importExcel(final String ds, final String collection, final String pathOrUrl) throws OperationException {
        final String filepath = pathOrUrl.startsWith("http:") ? downloadFile(pathOrUrl) : pathOrUrl;

        try {
            Workbook workbook = WorkbookFactory.create(new File(filepath));

        } catch (IOException | InvalidFormatException e) {
            e.printStackTrace();
            throw new OperationException(ErrorCode.EXCEL_DATA_READ_ERR, "Unable to read file at: " + filepath);
        }

    }

    private String downloadFile(final String filepath) throws OperationException {
        throw new OperationException(ErrorCode.OPERATION_NOT_SUPPORTED);
    }
}
