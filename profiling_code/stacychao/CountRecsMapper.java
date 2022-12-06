import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CountRecsMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    //private final static IntWritable zero = new IntWritable(0);
    private Text word = new Text("Total Lines: ");

    public void map(Object key, Text value, Context context)
     throws IOException, InterruptedException {
        try{
            context.write(word, one);
        }
        catch (IOException e){
            System.exit(0);
        }
        catch (InterruptedException e){
            System.exit(0);
        }
        
        
    }
    
}