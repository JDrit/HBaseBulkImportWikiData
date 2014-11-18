package edu.rit.csh.wikiData;

import java.io.IOException;
import java.util.Locale;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class HBaseKVMapper extends
    Mapper<LongWritable, Text, ImmutableBytesWritable, KeyValue> {
  ImmutableBytesWritable hKey = new ImmutableBytesWritable();
  KeyValue kv;

  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    String[] inputLine = value.toString().split(" ");
    String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
    hKey.set(inputLine[1].getBytes());
    context.write(hKey, new KeyValue(hKey.get(), "timestamps".getBytes(), 
                fileName.getBytes(), inputLine[2].getBytes()));
  }
}
