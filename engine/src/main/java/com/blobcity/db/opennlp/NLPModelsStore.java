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

import opennlp.tools.langdetect.LanguageDetectorModel;
import opennlp.tools.sentdetect.SentenceDetectorME;
import opennlp.tools.sentdetect.SentenceModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * Must be a Singleton
 *
 * @author sanketsarang
 */
@Component
public class NLPModelsStore {

    private static final Logger logger = LoggerFactory.getLogger(NLPModelsStore.class.getName());

    /**
     * Stores the langauge code against a corresponding langauge detector
     */
    private LanguageDetectorModel languageDetectorModel;

    /**
     * Stores the language code against a corresponding trained sentence detector
     */
    private final Map<String, SentenceDetectorME> sentenceDetectorMap = new HashMap<>();

    /**
     * Stores the language code against a corresponding trained tokenizer model
     */
    private final Map<String, Object> tokenizerMap = new HashMap<>();

    @PostConstruct
    public void startup() {
        initLangaugeDetectorModel();
        initSetenceDetector();
        initTokenizer();
    }

    private void initLangaugeDetectorModel() {
        InputStream is = null;
        try {
            is = new FileInputStream(NLPPathUtil.languageDetectorModelPath());
            languageDetectorModel = new LanguageDetectorModel(is);
            logger.debug("OpenNLP Language detector model loaded successful");
        } catch (FileNotFoundException e) {
            logger.warn("OpenNLP Language detector model failed to load. NLPImplementation functionality may not work correctly");
        } catch (IOException e) {
            e.printStackTrace();
            logger.warn("OpenNLP Language detector model failed to load. NLPImplementation functionality may not work correctly");
        }
    }

    private void initSetenceDetector() {
        SentenceModel sentenceModel;

        /* Load english sentence detector */
        sentenceModel = getSentenceModel(NLPLanguages.ENGLISH);
        if(sentenceModel != null) {
            sentenceDetectorMap.put(NLPLanguages.ENGLISH.getLanguageCode(), new SentenceDetectorME(sentenceModel));
            logger.debug("OpenNLP english sentence detector loaded successfully");
        }

        //TODO: Load models for every other language to be supported
    }

    private SentenceModel getSentenceModel(NLPLanguages language) {
        InputStream is = null;
        try {
            is = new FileInputStream(NLPPathUtil.setenceDetectorModelPath(NLPLanguages.ENGLISH));
            return new SentenceModel(is);
        } catch (FileNotFoundException e) {
            logger.warn("OpenNLP English sentence detector model failed to load. NLPImplementation functionality may not work correctly");
            return null;
        } catch (IOException e) {
            e.printStackTrace();
            logger.warn("OpenNLP English sentence detector model failed to load. NLPImplementation functionality may not work correctly");
            return null;
        }
    }

    /**
     * The OpenNLP Tokenizers segment an input character sequence into tokens. Tokens are usually words, punctuation,
     * numbers, etc. This function initliaises the tokenizer on a per language basis
     */
    private void initTokenizer() {

    }   //TODO: Intalise the tokeniser
}
