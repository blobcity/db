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

package com.blobcity.db.sql.lang;

/**
 * @author sanketsarang
 */
public class Aggregate<T extends Number> {
    T aggregate = (T) new Double(0.0);
    private boolean error = false;
    T min = (T) new Double(0.0);
    T max = (T) new Double(0.0);
    T total_count = (T) new Double(0.0);
    T avg = (T) new Double(0.0);

    public synchronized void add(T number) {
        aggregate = (T) new Double(aggregate.doubleValue() + number.doubleValue());
    }

    public synchronized void addCount(T number) {
        total_count = (T) new Double(total_count.doubleValue() + number.doubleValue());
    }

    public synchronized T getCount() {
        System.out.println(total_count);
        return total_count;
    }

    public synchronized T getAggregate() {
        return aggregate;
    }

    public synchronized void setAverage() {
        avg = (T) new Double(aggregate.doubleValue()/total_count.doubleValue());
    }

    public synchronized T getAverage() {
        return avg;
    }

    public synchronized void setError() {
        this.error = true;
    }

    public synchronized boolean isError() {
        return error;
    }

    public synchronized void setInitialMin(T number) {
        min = number;
    }

    public synchronized void findMin(T number) {
        if (number.doubleValue()<min.doubleValue())
            min = number;
    }

    public synchronized T getMin() {
        return min;
    }

    public synchronized void setInitialMax(T number) {
        max = number;
    }

    public synchronized void findMax(T number) {
        if (number.doubleValue()>max.doubleValue())
            max = number;
    }

    public synchronized T getMax() {
        return max;
    }
}