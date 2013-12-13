package basicExamples;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class Sqrt {

  public static class Map 
  extends MapReduceBase 
  implements Mapper<LongWritable, Text, DoubleWritable, DoubleWritable> {
    private Text word = new Text();

    public void map(LongWritable key, 
        Text value, 
        OutputCollector<DoubleWritable, DoubleWritable> output, 
        Reporter reporter) 
            throws IOException {
      String line = value.toString() ;
      DoubleWritable clave = new DoubleWritable()  ;
      DoubleWritable valor = new DoubleWritable() ;
      clave.set(Double.parseDouble(line));
      valor.set(Math.sqrt(Double.parseDouble(line))) ;
      output.collect(clave, valor) ; 
      }
    }
  
  public static class Reduce 
  extends MapReduceBase 
  implements Reducer<DoubleWritable, DoubleWritable, DoubleWritable, Text> {
    public void reduce(DoubleWritable key, 
        Iterator<DoubleWritable> values, 
        OutputCollector<DoubleWritable, Text> output, 
        Reporter reporter) 
            throws IOException {
      output.collect(key, new Text(values.next().toString() + " -- "));
    }
  }
  
  public static void main(String[] args) throws Exception {
    JobConf conf = new JobConf(Sqrt.class);
    conf.setJobName("sqrt");

    conf.setOutputKeyClass(DoubleWritable.class);
    conf.setOutputValueClass(DoubleWritable.class);

    conf.setMapperClass(Map.class);
    /*conf.setCombinerClass(Reduce.class);*/
    conf.setReducerClass(Reduce.class);

    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);

    FileInputFormat.setInputPaths(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));

    JobClient.runJob(conf);
  }
}
