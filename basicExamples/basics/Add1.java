package basicExamples;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class Add1 {

  public static class Map 
  extends MapReduceBase 
  implements Mapper<LongWritable, Text, IntWritable, IntWritable>{

    public void map(LongWritable key, 
        Text value,
        OutputCollector<IntWritable, IntWritable> output, 
        Reporter reporter)
            throws IOException {
      String line = value.toString() ;
      IntWritable clave = new IntWritable()  ;
      IntWritable valor = new IntWritable() ;
      clave.set(Integer.parseInt(line));
      valor.set(Integer.parseInt(line)+1) ;
      output.collect(clave, valor) ;      
      }
    }


  public static void main(String[] args) throws Exception {
    JobConf conf = new JobConf(Add1.class) ;
    conf.setJobName("sumar1") ;

    conf.setOutputKeyClass(IntWritable.class) ;
    conf.setOutputValueClass(IntWritable.class) ;

    conf.setMapperClass(Map.class) ;

    conf.setInputFormat(TextInputFormat.class) ;
    conf.setOutputFormat(TextOutputFormat.class) ;

    FileInputFormat.setInputPaths(conf, new Path(args[0])) ;
    FileOutputFormat.setOutputPath(conf, new Path(args[1])) ;

    JobClient.runJob(conf) ;
  }
}
