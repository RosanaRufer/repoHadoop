package publications;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class PublicationsMain extends Configured implements Tool {

  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = getConf() ;
    Job job = Job.getInstance(conf) ;
    
    job.setJarByClass(getClass()) ;
    job.setJobName(getClass().getSimpleName()) ;
        
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);

    job.setMapperClass(PublicationsMapper.class);
    /*job.setCombinerClass(LongSumReducer.class);*/
    job.setReducerClass(LongSumReducer.class);

    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    boolean result = job.waitForCompletion(true) ;
    return 0 ;
  }

  public static void main(String[] args) throws Exception {
    int result = ToolRunner.run(new PublicationsMain(), args) ;
  }
}
