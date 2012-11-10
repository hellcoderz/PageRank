
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


 public class PageRankSortMapper extends Mapper<LongWritable, Text, Text, Text> {

    public PageRankUtil util;
    private Text outKey = new Text();	//output key
    private Text outValue = new Text();	//output value
    private String double_separator = "\t\t";	//separator to identify outlinks line
    private String separator = "\t";	//normal separator to identify inlink, pagerank and outlinks

    public enum nodes{COUNT};
    public enum edges{COUNT};
    public enum min{COUNT};
    public enum max{COUNT};

    public void setup(Context ctx){
        ctx.getCounter(min.COUNT).setValue(9999);
    }
          
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
	{

            //increase no. of nodes by 1
            context.getCounter(nodes.COUNT).increment(1);
            //convert to string
       		String line = value.toString();
            String[] valuesList = line.split("\t");     //split string into substrings based on deliminator
            String current_link = valuesList[0];        //get current link
            String pagerank = valuesList[1]; // get the pagerank
            context.getCounter(edges.COUNT).increment(valuesList.length - 2);    //update no of edges which are eaqual to no. of outlinks
            if(context.getCounter(max.COUNT).getValue() < valuesList.length - 2)   //find max no. of outlinks
                context.getCounter(max.COUNT).setValue(valuesList.length - 2);
            if(context.getCounter(min.COUNT).getValue() > valuesList.length - 2);   //find min no. of outlinks
                context.getCounter(min.COUNT).setValue(valuesList.length - 2);
                context.getCounter(min.COUNT).setValue(0);

            Double pg = Double.parseDouble(pagerank);   //converting string to double
            pg = 1/pg;  //converting lowest rank to highest and vice-a-versa to sort the result in decreasing order
            context.write(new Text(String.valueOf(pg)), new Text(separator + current_link));    //emit pagerank,current link
	}
 }

