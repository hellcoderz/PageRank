
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
          
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
	{

            
            util.no_of_nodes = util.no_of_nodes + 1;
       		String line = value.toString();
                String[] valuesList = line.split("\t");
                String current_link = valuesList[0];
                String pagerank = valuesList[1]; // the first element is empty
                util.no_of_edges = util.no_of_edges + valuesList.length - 2;
                if(util.max_outlinks < valuesList.length - 2)
                    util.max_outlinks = valuesList.length - 2;
                if(util.min_outlinks > valuesList.length - 2)
                    util.min_outlinks = valuesList.length - 2;
                context.write(new Text(pagerank), new Text(separator + current_link));
	}
 }

