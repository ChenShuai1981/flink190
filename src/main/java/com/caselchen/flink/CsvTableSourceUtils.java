package com.caselchen.flink;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.sources.CsvTableSource;

import java.io.*;
import java.util.Arrays;
import java.util.List;

public class CsvTableSourceUtils {

    public static CsvTableSource genRatesHistorySource() {
        List<String> csvRecords = Arrays.asList(
            "rowtime ,currency   ,rate",
            "09:00:00   ,US Dollar  , 102",
            "09:00:00   ,Euro       , 114",
            "09:00:00  ,Yen        ,   1",
            "10:45:00   ,Euro       , 116",
            "11:15:00   ,Euro       , 119",
            "11:49:00   ,Pounds     , 108"
        );

        String tempFilePath = writeToTempFile(String.join("$", csvRecords), "csv_source_", "tmp");

        return new CsvTableSource(
          tempFilePath,
          new String[]{"rowtime","currency","rate"},
          new TypeInformation[]{
                  Types.STRING, Types.STRING, Types.STRING
          }, ",", "$", '`', true, "%", true
        );
    }

    public static String writeToTempFile(String contents, String filePrefix, String fileSuffix) {
        String result = null;
        try {
            File tempFile = File.createTempFile(filePrefix, fileSuffix);
            Writer tempWriter = new OutputStreamWriter(new FileOutputStream(tempFile), "UTF-8");
            tempWriter.write(contents);
            tempWriter.close();
            result = tempFile.getAbsolutePath();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }
}
