package ie.paco.hadoop.mapreduce;

import java.io.IOException;
import java.util.regex.Pattern;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

public class RegexRecordReader extends RecordReader<LongWritable, Text> {

    private LineRecordReader lineRecordReader = new LineRecordReader();
    private Pattern pattern;
    private Text value = new Text();

    public void setPattern(Pattern pattern2) {
        pattern=pattern2;
    }
 
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
        lineRecordReader.initialize(split, context);
    }
 	
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
     
           //while(lineRecordReader.nextKeyValue()) {
                //Matcher matcher;
 
                //matcher = pattern.matcher(lineRecordReader.getCurrentValue().toString());
 
                //if (matcher.find()) {
                  //int fieldCount;
                  //Text[] fields;
 
                  //fieldCount = matcher.groupCount();
                  //fields = new Text[fieldCount];
 
                  //for (int i = 0; i < fieldCount; i++) {
                    //fields[i] = new Text(matcher.group(i + 1));
                  //}
 
                  //value.setFields(fields);
                  //return true;
                //}
              //}
              return false;
    }
 
    @Override
    public LongWritable getCurrentKey() throws IOException,
            InterruptedException {
        return lineRecordReader.getCurrentKey();
    }
 
    @Override
    public Text getCurrentValue() throws IOException,
            InterruptedException {
        return value;
    }
 
    @Override
    public float getProgress() throws IOException, InterruptedException {
        return lineRecordReader.getProgress();
    }
 
    @Override
    public void close() throws IOException {
        lineRecordReader.close();
    }
}

