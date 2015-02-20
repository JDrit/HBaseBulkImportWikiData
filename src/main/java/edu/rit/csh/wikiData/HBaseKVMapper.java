package edu.rit.csh.wikiData;

import java.io.IOException;
import java.util.Locale;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyValueOutputFormat;
import org.apache.avro.generic.GenericData.Record;

/*
 * Takes input formated as: page title \t timestamp \t view count
 */
public class HBaseKVMapper extends
      Mapper<AvroKey<Record>, NullWritable, ImmutableBytesWritable, KeyValue> {
  
    ImmutableBytesWritable hKey = new ImmutableBytesWritable();
    KeyValue kv;
    public static final Log LOG = LogFactory.getLog(HBaseKVMapper.class);

    @Override
    protected void map(AvroKey<Record> key, NullWritable value, Context context) 
            throws IOException, InterruptedException {
        
        hKey.set(key.datum().get("pageTitle").toString().getBytes());
        context.write(hKey, new KeyValue(hKey.get(), "timestamps".getBytes(),
                key.datum().get("timestamp").toString().getBytes(), 
                key.datum().get("count").toString().getBytes()));
    }
}
