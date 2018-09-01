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

package com.blobcity.db.lang.functions;

import java.util.Iterator;
import java.util.NoSuchElementException;
import org.springframework.stereotype.Component;

/**
 *
 * @author sanketsarang
 */
@Component
public class FunctionBean {

    public long now() {
        return System.currentTimeMillis();
    }

    /**
     * Counts the number of elements present within the iterator
     * @param iterator The iterator who's size is sought
     * @return the number of elements present within the iterator, zero if no elements found to iterate
     */
    public long count(Iterator<Object> iterator) {
        long count = 0;
        try {
            iterator.next();
            count++;
        } catch (NoSuchElementException ex) {
            //do nothing
        }
        return count;
    }

    /**
     * Performs summation operation over all elements within the iterator for
     * <code>Integer</code> type
     *
     * @param iterator The elements to be summed in the form of an iterator
     * @return The summation of all elements within the iterator, Zero if iterator has no items to iterate
     */
    public int sumInt(Iterator<Integer> iterator) {
        int sum = 0;
        while (iterator.hasNext()) {
            sum += iterator.next();
        }
        return sum;
    }

    /**
     * Performs summation operation over all elements within the iterator for
     * <code>Float</code> type
     *
     * @param iterator The elements to be summed in the form of an iterator
     * @return The summation of all elements within the iterator, Zero if iterator has no items to iterate
     */
    public float sumFloat(Iterator<Float> iterator) {
        float sum = 0.0f;
        while (iterator.hasNext()) {
            sum += iterator.next();
        }
        return sum;
    }

    /**
     * Performs summation operation over all elements within the iterator for
     * <code>Double</code> type
     *
     * @param iterator The elements to be summed in the form of an iterator
     * @return The summation of all elements within the iterator, Zero if iterator has no items to iterate
     */
    public double sumDouble(Iterator<Double> iterator) {
        double sum = 0;
        while (iterator.hasNext()) {
            sum += iterator.next();
        }
        return sum;
    }

    /**
     * Returns the maximum value within the complete iteration set. If the iterator has no elements to iterate the
     * function will return
     * <code>Integer.MIN_VALUE</code>
     *
     * @param iterator The iterator over values amongst which max is to be found out
     * @return max value amongst all values within the iterator sequence, <code>Integer.MIN_VALUE</code> if the iterator
     * had no elements
     */
    public int maxInt(Iterator<Integer> iterator) {
        int val;
        int max = Integer.MAX_VALUE;
        while (iterator.hasNext()) {
            val = iterator.next();
            if (val > max) {
                val = max;
            }
        }

        return max;
    }
    
    public float maxFloat(Iterator<Float> iterator) {
        //TODO: Implement this
        throw new UnsupportedOperationException("Not supported yet.");
    }
    
    public double maxDouble(Iterator<Double> iterator) {
        //TODO: Implement this
        throw new UnsupportedOperationException("Not supported yet.");
    }
    
    public int minInt(Iterator<Integer> iterator) {
        //TODO: Implement this
        throw new UnsupportedOperationException("Not supported yet.");
    }
    
    public float minFloat(Iterator<Float> iterator) {
        //TODO: Implement this
        throw new UnsupportedOperationException("Not supported yet.");
    }
    
    public double minDouble(Iterator<Double> iterator) {
        //TODO: Implement this
        throw new UnsupportedOperationException("Not supported yet.");
    }
    
    //TODO: Add more functions here
}
