package basicExamples;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class Add1Double {

  public static class Map 
  extends MapReduceBase 
  implements Mapper<LongWritable, Text, DoubleWritable, DoubleWritable>{
    private Text word = new Text() ;

    public void map(LongWritable key, 
        Text value,
        OutputCollector<DoubleWritable, DoubleWritable> output, 
        Reporter reporter)
            throws IOException {
      String line = value.toString() ;
      DoubleWritable clave = new DoubleWritable()  ;
      DoubleWritable valor = new DoubleWritable() ;
      clave.set(Integer.parseInt(line));
      valor.set(Integer.parseInt(line)+1.0) ;
      output.collect(clave, valor) ;      
      }
    }


  public static void main(String[] args) throws Exception {
    JobConf conf = new JobConf(Add1Double.class) ;
    conf.setJobName("sumar1") ;

    conf.setOutputKeyClass(DoubleWritable.class) ;
    conf.setOutputValueClass(DoubleWritable.class) ;

    conf.setMapperClass(Map.class) ;

    conf.setInputFormat(TextInputFormat.class) ;
    conf.setOutputFormat(TextOutputFormat.class) ;

    FileInputFormat.setInputPaths(conf, new Path(args[0])) ;
    FileOutputFormat.setOutputPath(conf, new Path(args[1])) ;

    JobClient.runJob(conf) ;
  }
}
