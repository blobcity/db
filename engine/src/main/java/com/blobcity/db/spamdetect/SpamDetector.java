package com.blobcity.db.spamdetect;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Created by sanketsarang on 13/06/18.
 */
@Component
public class SpamDetector implements com.blobcity.lib.functions.spam.SpamDetector {

    private static final Logger logger = LoggerFactory.getLogger(SpamDetector.class);

    private final NaiveBayesClassifier nb = new NaiveBayesClassifier();
    private final ClassifierEvaluator eval = new ClassifierEvaluator(nb);

    private boolean requiresTraining = true;

    @Override
    public boolean isSpam(String message, String title) {

        if(requiresTraining) {
            train();
        }

        message = message.trim();

        if(title != null) {
            title = title.trim();
            return (nb.classify(message) || nb.classify(title));
        }

        return nb.classify(message);
    }

    public void train() {
        eval.Evaluate(true);
        logger.info("Spam Detection: Accuracy: {}, Precision: {}, Recall: {}, FMeasure: {}", eval.getAccuracy(), eval.getPrecision(), eval.getRecall(), eval.getFMeasure());
        requiresTraining = false;
    }

    public void setRequiresTraining() {
        requiresTraining = true;
    }
}
