package tn.enit.tp1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
//Most Used Payment Method

public class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text paymentMethod = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Split the line by tab
        String[] fields = value.toString().split("\t");

        // Ensure the line has at least 6 columns (payment method is the 6th column)
        if (fields.length > 5) {
            String methodOfPayment = fields[5]; // Extract the payment method (6th column)
            paymentMethod.set(methodOfPayment);
            context.write(paymentMethod, one); // Emit payment method with count 1
        }
    }
}

