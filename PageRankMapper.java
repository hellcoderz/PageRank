
import java.io.IOException;

        
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


 public class PageRankMapper extends Mapper<LongWritable, Text, Text, Text> {
    
    private Text outKey = new Text();	//output key
    private Text outValue = new Text();	//output value
    private String double_separator = "\t\t";	//separator to identify outlinks line
    private String separator = "\t";	//separator to separate links
          
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
       		String line = value.toString();		//convert Text to string
                int separateIndex = line.indexOf("\t");		//
                String current_link = line.substring(0, separateIndex);
                String rest = line.substring(separateIndex, line.length()); // get the rest of the line
                String[] valuesList = rest.split("\t"); // the array contains the PageRank and the outlinks
                String pagerank = valuesList[1]; // the first element is empty
                
                StringBuilder sb = new StringBuilder(); // save the outlinks
                outValue.set(pagerank + "\t" + current_link + "\t" + (valuesList.length-2) + "\t");
                for (int i = 2; i < valuesList.length; i++) { // start from the third element since the second is PageRank
                        outKey.set(valuesList[i]);
                        context.write(outKey, outValue);
                        sb.append(valuesList[i] + separator);
                }
                
                context.write(new Text(current_link), new Text(double_separator + sb.toString()));
    }
 }

