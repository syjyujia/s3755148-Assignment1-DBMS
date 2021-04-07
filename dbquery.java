import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

/**
 * utility for query data from heap file
 * <p>
 *     java dbquery querytext pagesize
 *     e.g. java dbquery "1711/01/2019 06:00:00 PM" 2048
 * </p>
 */
public class dbquery {

    public static void main(String[] args) {

        if (args == null || args.length != 2) {
            System.err.println("java dbquery querytext pagesize");
            return;
        }

        Integer pageSize = Integer.parseInt(args[1].trim());
        String heapFilePath = "heap." + pageSize;
        File heapFile = new File(heapFilePath);
        if (!heapFile.exists()) {
            System.err.println("cannot find heap file");
            return;
        }

        String queryText = args[0];

        FileInputStream fileInputStream = null;
        try {

            long start = System.currentTimeMillis();

            // open heap file
            fileInputStream = new FileInputStream(heapFile);

            // initialize byte buffer
            byte[] buffer = new byte[pageSize];

            // total count of records matching the query text
            int totalCount = 0;

            // read the heap file
            while (fileInputStream.read(buffer) >= 0) {

                // find matched data in current page data
                int count = pageQuery(buffer, queryText, pageSize);
                totalCount += count;
            }

            long end = System.currentTimeMillis();
            System.out.println("find " + totalCount + " records in " + (end - start) + "ms.");

        } catch (Exception e) {
            System.err.println("open heap file failed");
            e.printStackTrace();
        } finally {
            try {
                fileInputStream.close();
            } catch (IOException e) {
                System.err.println("close file input stream failed");
                e.printStackTrace();
            }
        }
    }

    /**
     * query in current page
     * @param pageData page data
     * @param queryText query text
     * @param pageSize
     */
    private static int pageQuery(byte[] pageData, String queryText, Integer pageSize) {

        // record count matches query text
        int recordCount = 0;

        // the first record starts from the fourth byte of the page, exclude page record count
        int recordStartIndex = 4;

        while (true) {
            if (recordStartIndex + 4 >= pageSize) {
                break;
            }

            // id: 0th~3rd bytes
            int id = byteArrayToInt(getSubArray(pageData, recordStartIndex, 4));
            if (id == 0) {
                // reach end of page
                break;
            }

            // sensor_name_length: 20th~24th bytes
            int sensorNameLength = byteArrayToInt(getSubArray(pageData, recordStartIndex + 30, 4));
            // Sensor_Name
            String sensorName = new String(getSubArray(pageData, recordStartIndex + 34, sensorNameLength));
            // SDT_NAME start position
            int sdtNameIndex = recordStartIndex + 34 + sensorNameLength;
            // convert SDT_NAME_length bytes to int value
            int sdtNameLength = byteArrayToInt(getSubArray(pageData, sdtNameIndex, 4));
            // SDT_NAME string
            String sdtName = new String(getSubArray(pageData, sdtNameIndex + 4, sdtNameLength));

            // sensor_id: 12th~15th bytes
            int sensorId = byteArrayToInt(getSubArray(pageData, recordStartIndex + 22, 4));
            // Hourly_Counts: 16th~19th bytes
            int hourlyCounts = byteArrayToInt(getSubArray(pageData, recordStartIndex + 26, 4));

            // data match
            if (queryText.equals(sdtName)) {
                System.out.println("ID=" + id + ", Sensor_Id=" + sensorId + ", Sensor_Name=" + sensorName
                        + ", SDT_NAME=" + sdtName + ", Hourly_Counts=" + hourlyCounts);
                recordCount ++;
            }

            // move to the start index of next record
            recordStartIndex += (38 + sensorNameLength + sdtNameLength);
        }

        return recordCount;
    }

    /**
     * get the sub array of bytes from startIndex to startIndex + length
     * @param bytes the base type array
     * @param startIndex the start index of base array
     * @param length the length of sub array
     * @return
     */
    private static byte[] getSubArray(byte[] bytes, int startIndex, int length) {
        byte[] result = new byte[length];
        for (int i = 0; i < length; i++) {
            result[i] = bytes[startIndex + i];
        }
        return result;
    }

    /**
     * transfer a byte array data into an int value
     * @param bytes the original byte array
     * @return the converted int value
     */
    public static int byteArrayToInt(byte[] bytes) {
        int value = 0;
        for (int i = 0; i < 4; i++) {
            int shift = (3 - i) * 8;
            value += (bytes[i] & 0xFF) << shift;
        }
        return value;
    }
}
