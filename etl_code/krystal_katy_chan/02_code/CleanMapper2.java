import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CleanMapper2 extends Mapper<LongWritable, Text, Text, Text>{
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line [] = value.toString().split(",");
        //check number of columns
        if (line.length == 15) {
            //extract zipcode + ensure correct values 
            String zipcode = line[12]; 

            //filter values for nyc 
            String county = line[0].toUpperCase(); 
            // String city = line[10].toUpperCase();  
            if ( (zipcode.matches("\\d+")) ) {
                if (county.equals("NEW YORK") || (county.equals("KINGS")) || (county.equals("BRONX")) || (county.equals("RICHMOND")) || (county.equals("QUEENS")) ) {
                    //drop other columns 
                    String res = county + "," + line[1] +"," + line[3] + "," +line[4] + ","+line[5] + "," + line[10] + "," +zipcode; 
                    context.write(new Text(res), new Text("")); 
                }
            }
        }
    }
}