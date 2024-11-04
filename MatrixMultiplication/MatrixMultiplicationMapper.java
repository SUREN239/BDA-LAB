import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MatrixMultiplicationMapper extends Mapper<Object, Text, Text, IntWritable> {
    private boolean isMatrixA;
    private int matrixSize;

    @Override
    protected void setup(Context context) {
        // Assuming the matrix size (for example, 3 for a 3x3 matrix) is provided via configuration
        matrixSize = context.getConfiguration().getInt("matrix.size", 3);
        isMatrixA = context.getConfiguration().getBoolean("isMatrixA", true);
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split(",");
        int i = Integer.parseInt(tokens[0]);
        int j = Integer.parseInt(tokens[1]);
        int matrixValue = Integer.parseInt(tokens[2]);

        for (int k = 0; k < matrixSize; k++) {
            if (isMatrixA) {
                // Emit for Matrix A: (i,k) as key and (A,j,value) as value
                context.write(new Text(i + "," + k), new IntWritable(matrixValue));
            } else {
                // Emit for Matrix B: (k,j) as key and (B,i,value) as value
                context.write(new Text(k + "," + j), new IntWritable(matrixValue));
            }
        }
    }
}
