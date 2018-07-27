package bigdata.course.hw2.ipbytes.util;

import eu.bitwalker.useragentutils.UserAgent;

/**
 * The util class that is used to parse records from the dataset and
 * to get user's browser (with UserAgent lib help)
 */
public class RecordParser {

    private int ip;
    private int bytes;
    private String browser;

    /**
     * Checks the record for validity and parses it
     *
     * @param record - record to parse
     * @return - true, if the record is valid, false otherwise
     */
    public boolean parseRecord(String record) {

        if (record.trim().length() == 0) {
            return false;
        }

        String[] recordSplittedBySpaces = record.split("\\s");
        String[] recordSplittedByQuotes = record.split("\"");

        try {

            ip = Integer.parseInt(recordSplittedBySpaces[0].substring(2));

            String stringBytes = recordSplittedBySpaces[9];
            if (stringBytes.equals("-")) {
                bytes = 0;
            } else {
                bytes = Integer.parseInt(stringBytes);
            }

            browser = UserAgent.parseUserAgentString(recordSplittedByQuotes[5]).getBrowser().toString();

        } catch (ArrayIndexOutOfBoundsException | NumberFormatException e) {
            return false;
        }

        return true;
    }

    public int getIp() {
        return ip;
    }

    public int getBytes() {
        return bytes;
    }

    public String getBrowser() {
        return browser;
    }

}




