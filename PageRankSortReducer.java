import java.io.IOException;
        
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

 public class PageRankSortReducer extends Reducer<Text, Text, Text, Text> {
	private String vals = "";
    public void reduce(Text key, Iterable<Text> values, Context context) 
      throws IOException, InterruptedException {
		for(Text value: values){
			context.write(new Text(String.valueOf(1/Double.parseDouble(key.toString()))), value);	//to emit actual rank and node
		} 
     }
 }

