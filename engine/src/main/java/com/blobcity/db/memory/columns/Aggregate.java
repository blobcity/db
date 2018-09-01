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

package com.blobcity.db.memory.columns;

/**
 * @author sanketsarang
 */
public class Aggregate {
    private double count;
    private double sum;
    private double max;
    private double min;
    private double avg;

    private boolean maxRequiresRecompute = false;
    private boolean minRequiresRecompute = false;
    private boolean avgRequiresRecompute = false;

    public void addElement(final double element) {
        count ++;
        sum += element;
    }

    public void removeElement(final double element) {
        count --;
        sum -= element;
    }

    public void recompute(final Column column) {
        //TODO: implement this to recompute min, max and avg
    }

}
