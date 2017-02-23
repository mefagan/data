import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PageRankMapper
	extends Mapper<LongWritable, Text, Text, Text>
{
	
	@Override
	public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException{
		String line = value.toString();
        String[] tokens = line.split(" ");
        int tokenSize = tokens.length-1;
        StringBuilder links = new StringBuilder();
        float pagerank;
		for (int i = 1; i < tokenSize; i++) {
            pagerank = (Float.parseFloat(tokens[tokenSize] / (tokenSize -1));
            context.write(new Text(tokens[i], new Text(tokens[0] + "," + pagerank.toString() )));
            links.append(tokens[i] + " ");
        }
        context.write(new Text(tokens[0], new Text(links.toString())));
    }
}
