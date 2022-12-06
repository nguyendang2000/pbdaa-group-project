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

public class CleanMapper extends Mapper<Object, Text, Text, IntWritable> {
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] linePart = line.split("\t");
        if(linePart.length > 14){
            String ID = linePart[0];
            String zipCode = linePart[5];
            String grade = linePart[14].trim();
            String date = linePart[8];
            String violation = linePart[11];
            if(!grade.isEmpty()) {
                if(!zipCode.isEmpty()) {
                    String dateStr = linePart[8];
                    String[] arr = dateStr.split("/");
                    if(arr.length == 3 && arr[2].equals("2022")) {
                        context.write(new Text( ID + ","+ zipCode + ","+ date + ","+ grade + "," + "\"" + violation + "\""), new IntWritable(0));
                    }
                }
            }
        }
    }
}