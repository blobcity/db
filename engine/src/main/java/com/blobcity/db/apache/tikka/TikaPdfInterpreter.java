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

package com.blobcity.db.apache.tikka;

import com.blobcity.db.exceptions.ErrorCode;
import com.blobcity.db.exceptions.OperationException;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.sax.BodyContentHandler;
import org.apache.tika.parser.pdf.PDFParser;
import org.json.JSONObject;
import org.xml.sax.SAXException;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * @author sanketsarang
 */
public class TikaPdfInterpreter implements TikaInterpreter {

    @Override
    public String toText(String filePath) throws OperationException {
        BodyContentHandler handler = new BodyContentHandler();
        Metadata metadata = new Metadata();
        FileInputStream inputstream = null;
        try {
            inputstream = new FileInputStream(new File(filePath));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "Attempted to interpret an in-existent file");
        }
        ParseContext pcontext = new ParseContext();

        //parsing the document using PDF parser
        PDFParser pdfparser = new PDFParser();
        try {
            pdfparser.parse(inputstream, handler, metadata, pcontext);
        } catch (IOException | SAXException | TikaException e) {
            e.printStackTrace();
            throw new OperationException(ErrorCode.DATAINTERPRETER_EXECUTION_ERROR, "Failed to read file marked as PDF as it is not a PDF");
        }

        String[] metadataNames = metadata.names();

        for(String name : metadataNames) {
            System.out.println(name+ " : " + metadata.get(name));
        }

        return handler.toString();
    }

    @Override
    public JSONObject toJson(String filePath) throws OperationException {
        BodyContentHandler handler = new BodyContentHandler();
        Metadata metadata = new Metadata();
        FileInputStream inputstream = null;
        try {
            inputstream = new FileInputStream(new File(filePath));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            throw new OperationException(ErrorCode.INTERNAL_OPERATION_ERROR, "Attempted to interpret an in-existent file");
        }
        ParseContext pcontext = new ParseContext();

        //parsing the document using PDF parser
        PDFParser pdfparser = new PDFParser();
        try {
            pdfparser.parse(inputstream, handler, metadata, pcontext);
        } catch (IOException | SAXException | TikaException e) {
            e.printStackTrace();
            throw new OperationException(ErrorCode.DATAINTERPRETER_EXECUTION_ERROR, "Failed to read file marked as PDF as it is not a PDF");
        }

        JSONObject jsonObject = new JSONObject();
        jsonObject.put("_txt", handler.toString());

        String[] metadataNames = metadata.names();
        for(String name : metadataNames) {
            jsonObject.put(name, metadata.get(name));
        }

        return jsonObject;
    }
}
