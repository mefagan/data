import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PageRankReducer
        extends Reducer<Text, Text, Text, Text>
{       
        @Override
        public void reduce (Text key, Iterable<IntWritable> values, Context context)    
                throws IOException, InterruptedException{
                        
            String links = "";
            String input = "";
            float pagerank = 0;
            String comma = ",";
    for (Text value : values){
                input = value.toString();
                if (input.contains(comma))) {
                    pr = Float.parseFloat(input.split(comma)[1]);
                } else { 
                    links = input;
                }   
                        }
                        
            context.write(key, new Text(links + pagerank));
                }
}               


