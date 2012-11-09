import java.io.IOException;
        
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

 public class PageRankReducer extends Reducer<Text, Text, Text, Text> {
	private double dampFactor = 0.85; 
        private Text outputValue = new Text();
        private String separator = " ";
        

    public void reduce(Text key, Iterable<Text> values, Context context) 
      throws IOException, InterruptedException {
		double sum = 0;
		
                String outlinks = "";
                for (Text val : values) {
                        String temp = val.toString();           
                        if (temp.indexOf("\t\t") == 0) {
                                outlinks = temp.substring(temp.indexOf("\t\t")+1, temp.length());
                        }
                        else {
                                String[] array = temp.split("\t");
                                sum += Double.valueOf(array[0])/Double.valueOf(array[2]);
                        }
                }

                sum = dampFactor * sum + (1 - dampFactor);
                outputValue.set(String.valueOf(sum) + outlinks);
                context.write(key, outputValue);
        
        
    }
 }

