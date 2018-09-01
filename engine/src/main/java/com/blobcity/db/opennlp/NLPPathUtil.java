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

package com.blobcity.db.opennlp;

import com.blobcity.db.constants.BSql;

/**
 * @author sanketsarang
 */
public class NLPPathUtil {

    public static String languageDetectorModelPath() {
        StringBuilder path = new StringBuilder(BSql.BSQL_BASE_FOLDER);
        path.append(BSql.RESOURCES);
        path.append(BSql.SEPERATOR);
        path.append(BSql.NLP_PRELOADED_MODELS_FOLDER);
        path.append(BSql.SEPERATOR);
        path.append(BSql.NLP_LANG_DETECT_FOLDER);
        path.append(BSql.SEPERATOR);
        path.append(BSql.NLP_LANG_DETECT_FILENAME);
        return path.toString();
    }

    public static String setenceDetectorModelPath(NLPLanguages language) {
        StringBuilder path = new StringBuilder(BSql.BSQL_BASE_FOLDER);
        path.append(BSql.RESOURCES);
        path.append(BSql.SEPERATOR);
        path.append(BSql.NLP_PRELOADED_MODELS_FOLDER);
        path.append(BSql.SEPERATOR);
        path.append(BSql.NLP_SENTENCE_DETECT_FOLDER);
        path.append(BSql.SEPERATOR);
        path.append(language.getLanguageCode());
        path.append(BSql.NLP_SETENCE_DETECT_LANGUAGE_FILENAME);
        return path.toString();
    }

}
