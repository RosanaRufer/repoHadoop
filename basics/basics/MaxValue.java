package basicExamples;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import basicExamples.Add1.Map;

public class MaxValue {
  public static class Map 
  extends MapReduceBase 
  implements Mapper<LongWritable, Text, Text, IntWritable>{
    private Text word = new Text("MAX") ;

    public void map(LongWritable key, 
        Text value,
        OutputCollector<Text, IntWritable> output, 
        Reporter reporter)
            throws IOException {
      IntWritable number = new IntWritable( );
      String line = value.toString() ;
      StringTokenizer tokenizer = new StringTokenizer(line) ;
      int max = Integer.MIN_VALUE ;
      while (tokenizer.hasMoreTokens()) {
        int v = Integer.valueOf(tokenizer.nextToken()) ;
        if (v > max) 
          max = v ;
      }
      number.set(max);
      output.collect(word, number) ;    
    }
  }

  public static class Reduce 
  extends MapReduceBase 
  implements Reducer<Text, IntWritable, Text, IntWritable> {
    public void reduce(Text key, 
        Iterator<IntWritable> values, 
        OutputCollector<Text, IntWritable> output, 
        Reporter reporter) 
            throws IOException {
      int max = Integer.MIN_VALUE ;
      while (values.hasNext()) {
        int v = values.next().get();
        if (v > max)
          max = v ;
      }
      output.collect(key, new IntWritable(max));
    }
  }
  public static void main(String[] args) throws Exception {
    JobConf conf = new JobConf(MaxValue.class) ;
    conf.setJobName("maxvalue") ;

    conf.setOutputKeyClass(Text.class) ;
    conf.setOutputValueClass(IntWritable.class) ;

    conf.setMapperClass(Map.class) ;
    conf.setReducerClass(Reduce.class) ;

    conf.setInputFormat(TextInputFormat.class) ;
    conf.setOutputFormat(TextOutputFormat.class) ;

    FileInputFormat.setInputPaths(conf, new Path(args[0])) ;
    FileOutputFormat.setOutputPath(conf, new Path(args[1])) ;

    JobClient.runJob(conf) ;
  }
}
