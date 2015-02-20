package edu.rit.csh.wikiData;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil; 
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyValueOutputFormat;
import edu.rit.csh.LogRecord;

import org.apache.avro.*;
import org.apache.avro.Schema.Type;
import org.apache.avro.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;

/**
 * HBase bulk import example<br>
 * Data preparation MapReduce job driver
 * <ol>
 * <li>args[0]: HDFS input path
 * <li>args[1]: HDFS output path
 * <li>args[2]: HBase table name
 * </ol>
 */
public class Driver extends Configured implements Tool {

    public int run(String[] args) throws Exception {
    Configuration conf = getConf();
    //Configuration conf = new Configuration();
    args = new GenericOptionsParser(conf, args).getRemainingArgs();

    conf.setInt("epoch.seconds.tipoff", 1275613200);
    conf.set("hbase.table.name", args[2]);
    
    // Deletes the output dir
    FileSystem fs = FileSystem.get(conf);
    fs.delete(new Path(args[1]), true);
    
    // Load hbase-site.xml 
    HBaseConfiguration.addHbaseResources(conf);

    Job job = new Job(conf, "HBase Bulk Import of Wiki Data");
    job.setJarByClass(HBaseKVMapper.class);
    
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.setInputFormatClass(AvroKeyInputFormat.class);
    job.setMapperClass(HBaseKVMapper.class);
    AvroJob.setInputKeySchema(job, LogRecord.getClassSchema());
    job.setMapOutputKeyClass(ImmutableBytesWritable.class);
    job.setMapOutputValueClass(KeyValue.class);
    //AvroJob.setOutputKeySchema(job, Schema.create(Schema.Type.BYTES));
    //AvroJob.setOutputValueSchema(job, Schema.create(Schema.Type.INT));

    // Auto configure partitioner and reducer
    HTable hTable = new HTable(conf, args[2].getBytes());
    HFileOutputFormat.configureIncrementalLoad(job, hTable);

    job.waitForCompletion(true);

    //fs.setPermission(new Path(args[1]), FsPermission.valueOf("drwxrwxrwx"));
    Runtime rt = Runtime.getRuntime();
    rt.exec("hadoop fs -chmod -R 777 " + args[1]);

    // Load generated HFiles into table
    LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);
    loader.doBulkLoad(new Path(args[1]), hTable);

    fs.delete(new Path(args[1]), true);
    return 0;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new Driver(), args);
    System.exit(res);
  }
}
