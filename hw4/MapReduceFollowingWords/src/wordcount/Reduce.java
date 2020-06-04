package wordcount;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Reduce extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text word, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        StringBuilder followingWords = new StringBuilder();
        for (Text value : values) {
            followingWords.append(value.toString());
        }
        context.write(word, new Text(followingWords.toString()));
    }
}
