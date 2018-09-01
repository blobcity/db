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

package com.blobcity.db.bif.nlp;

import com.blobcity.db.bif.nlp.Language;
import com.blobcity.db.bif.nlp.NLP;

import java.util.List;

/**
 * @author sanketsarang
 */
public class NLPImplementation implements NLP{

    @Override
    public Language detectLanguage(String s) {
        return null;
    }

    @Override
    public List<String> getSentences(Language language, String s) {
        return null;
    }

    @Override
    public List<String> getSentences(String s) {
        return null;
    }

    @Override
    public List<String> tokenize(Language language, String s) {
        return null;
    }

    @Override
    public List<String> tokenize(String s) {
        return null;
    }

    @Override
    public List<String> extractNames(Language language, String s) {
        return null;
    }

    @Override
    public List<String> extractNames(String s) {
        return null;
    }

    @Override
    public String markNames(Language language, String s) {
        return null;
    }

    @Override
    public String markNames(String s) {
        return null;
    }

    @Override
    public List<String> extractOrgNames(Language language, String s) {
        return null;
    }

    @Override
    public List<String> extractOrgNames(String s) {
        return null;
    }

    @Override
    public String markOrgNames(Language language, String s) {
        return null;
    }

    @Override
    public String markOrgNames(String s) {
        return null;
    }

    @Override
    public List<String> extractDates(Language language, String s) {
        return null;
    }

    @Override
    public List<String> extractDates(String s) {
        return null;
    }

    @Override
    public String markDates(Language language, String s) {
        return null;
    }

    @Override
    public String markDates(String s) {
        return null;
    }

    @Override
    public List<String> extractTimes(Language language, String s) {
        return null;
    }

    @Override
    public List<String> extractTimes(String s) {
        return null;
    }

    @Override
    public String markTimes(Language language, String s) {
        return null;
    }

    @Override
    public String markTimes(String s) {
        return null;
    }
}
