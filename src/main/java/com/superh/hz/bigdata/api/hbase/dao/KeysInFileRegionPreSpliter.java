package com.superh.hz.bigdata.api.hbase.dao;

import org.apache.hadoop.hbase.util.Bytes;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;
import java.util.TreeSet;

public class KeysInFileRegionPreSpliter implements RegionPreSpliter {

    public byte[][] calcSplitKeys(int regionCount) {
        TreeSet<byte[]> rows = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);

        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader("CONF_BASE_PATH/HBASE_TABLE_ROWKEY_SAMPLE_FILENAME"));
            String tempString = null;
            // 一次读入一行，直到读入null为文件结束
            while ((tempString = reader.readLine()) != null) {
                String[] plateNumberArray = tempString.split(",");
                for (String plateNumber : plateNumberArray) {
                    if (plateNumber != null && plateNumber.length() > 0)
                        rows.add(Bytes.toBytes(plateNumber));
                }
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                }
            }
        }

        int splitKeysNumber = regionCount - 1;
        int splitKeysBase = rows.size() / regionCount;
        byte[][] splitKeys = new byte[splitKeysNumber][];


        int pointer = 0;
        Iterator<byte[]> rowKeyIter = rows.iterator();

        int index = 0;
        while (rowKeyIter.hasNext()) {
            byte[] tempRow = rowKeyIter.next();
            rowKeyIter.remove();
            if ((pointer != 0) && (pointer % splitKeysBase == 0)) {
                if (index < splitKeysNumber) {
                    splitKeys[index] = tempRow;
                    index++;
                }
            }
            pointer++;
        }

        rows.clear();
        return splitKeys;
    }
}
