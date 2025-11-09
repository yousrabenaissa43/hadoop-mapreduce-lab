package tn.enit.tp1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


// Mapper class for counting sales per store
public class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text storeName = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Split the line by tab
        String[] fields = value.toString().split("\t");

        // Ensure the line has at least 4 columns (store name is the 3rd column)
        if (fields.length > 2) {
            String store = fields[2]; // Extract the store name (3rd column)
            storeName.set(store);
            context.write(storeName, one); // Emit store name with count 1
        }
    }
}

