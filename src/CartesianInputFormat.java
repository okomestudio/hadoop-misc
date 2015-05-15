package net.okomestudio.hadoop.mapred;

import java.io.IOException;
import java.lang.StringBuilder;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.join.CompositeInputSplit;
import org.apache.hadoop.util.ReflectionUtils;

import net.okomestudio.hadoop.mapred.CartesianRecordReader;



@SuppressWarnings({ "rawtypes", "static-access", "unchecked" })
public class CartesianInputFormat extends FileInputFormat {
  public static final String L_INPUT_FORMAT = "org.apache.hadoop.mapred.TextInputFormat";
  public static final String R_INPUT_FORMAT = "org.apache.hadoop.mapred.TextInputFormat";
  
  @Override
  public InputSplit[] getSplits(JobConf conf, int numSplits)
    throws IOException {
    try {
      String[] dirs = conf.get("mapred.input.dir").split(",");
      InputSplit[] leftSplits = getInputSplits(conf, L_INPUT_FORMAT, dirs[0], numSplits);
      InputSplit[] rightSplits = getInputSplits(conf, R_INPUT_FORMAT, dirs[1], numSplits);
      
      CompositeInputSplit[] splits
        = new CompositeInputSplit[leftSplits.length * rightSplits.length];
      
      int i = 0;
      for (InputSplit left : leftSplits) {
        for (InputSplit right : rightSplits) {
          splits[i] = new CompositeInputSplit(2);
          splits[i].add(left);
          splits[i].add(right);
          ++i;
        }
      }
      
      //LOG.info("Total splits to process: " + splits.length);
      return splits;
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
      throw new IOException(e);
    }
  }
  
  @Override
  public RecordReader getRecordReader(InputSplit split, JobConf conf, Reporter reporter)
    throws IOException {
    return new CartesianRecordReader((CompositeInputSplit)split, conf, reporter);
  }
  
  private InputSplit[] getInputSplits(JobConf conf, String inputFormatClass, String inputPath,
                                      int numSplits)
    throws ClassNotFoundException, IOException {
    FileInputFormat inputFormat
      = (FileInputFormat)ReflectionUtils.newInstance(Class.forName(inputFormatClass), conf);
    inputFormat.setInputPaths(conf, inputPath);
    return inputFormat.getSplits(conf, numSplits);
  }
}
