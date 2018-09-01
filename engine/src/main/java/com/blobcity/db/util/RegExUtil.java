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

package com.blobcity.db.util;

import com.blobcity.db.exceptions.ErrorCode;
import com.blobcity.db.exceptions.OperationException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
/**
 * @author sanketsarang
 */
public class RegExUtil {
    
    public static boolean isIntlPhoneNumber(String pPhoneNumber) {

        String regex = "^\\(?([0-9]{3})\\)?[-.\\s]?([0-9]{3})[-.\\s]?([0-9]{4})$";
 
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(pPhoneNumber);

        return matcher.matches();
    }
    
    public static boolean isIndianPhoneNumber(String pPhoneNumber) {

        String regex = "^\\+?\\(?([0-9]{0,2})\\)?[-.\\s]?\\(?([0-9]{2,3})\\)?[-.\\s]?([0-9]{4})[-.\\s]?([0-9]{4})$";
 
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(pPhoneNumber);

        return matcher.matches();
    }
    
    public static boolean isPhoneNumber(String number) {
        return isIntlPhoneNumber(number)|| isIndianPhoneNumber(number);
    }
    
    public static boolean isEmail(String email) {
        String regex = "^[_A-Za-z0-9-]+(\\.[_A-Za-z0-9-]+)*@[A-Za-z0-9]+(\\.[A-Za-z0-9]+)*(\\.[A-Za-z]{2,})$";
 
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(email);

        return matcher.matches();
    }
    
    public static boolean isIPAddress(String addr) {
        String regex = "^([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.([01]?\\d\\d?|2[0-4]\\d|25[0-5])$";
 
 
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(addr);

        return matcher.matches();
    }
    
    public static boolean isTimeTwelveHourFormat(String time) {
        String regex = "(1[012]|[1-9]):[0-5][0-9](\\\\s)?(?i)(am|pm)";
 
 
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(time);

        return matcher.matches();
    }
    
    public static boolean isTimeTwentyFourHourFormat(String time) {
        String regex = "([01]?[0-9]|2[0-3]):[0-5][0-9]";
 
 
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(time);

        return matcher.matches();
    }
    
    public static boolean isTimeFormat(String time) {
        return isTimeTwelveHourFormat(time) || isTimeTwentyFourHourFormat(time);
    }
    
    public static boolean isDateFormat(String date) {
        String regex = "([01]?[0-9]|2[0-3]):[0-5][0-9]";
 
 
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(date);

        return matcher.matches();
    }
}
