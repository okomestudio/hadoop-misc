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

import net.okomestudio.hadoop.mapred.CartesianInputFormat;



@SuppressWarnings({ "rawtypes", "static-access", "unchecked" })
public class CartesianRecordReader<K1, V1, K2, V2>
  implements RecordReader<Text, Text> {
  // record readers to get key value pairs
  private RecordReader leftRR = null, rightRR = null;
  
  // store configuration to re-create the right record reader
  private FileInputFormat rightFIF;
  private JobConf rightConf;
  private InputSplit rightIS;
  private Reporter rightReporter;
  
  // helper variables
  private K1 lkey;
  private V1 lvalue;
  private K2 rkey;
  private V2 rvalue;
  private boolean goToNextLeft = true, alldone = false;
  
  /**
   * Creates a new instance of the CartesianRecordReader
   * 
   * @param split
   * @param conf
   * @param reporter
   * @throws IOException
   */
  public CartesianRecordReader(CompositeInputSplit split, JobConf conf, Reporter reporter)
    throws IOException {
    this.rightConf = conf;
    this.rightIS = split.get(1);
    this.rightReporter = reporter;
    
    try {
      FileInputFormat leftFIF = (FileInputFormat)ReflectionUtils
        .newInstance(Class.forName(CartesianInputFormat.L_INPUT_FORMAT), conf);
      leftRR = leftFIF.getRecordReader(split.get(0), conf, reporter);
      
      rightFIF = (FileInputFormat)ReflectionUtils
        .newInstance(Class.forName(CartesianInputFormat.R_INPUT_FORMAT), conf);
      rightRR = rightFIF.getRecordReader(rightIS, rightConf, rightReporter);
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
      throw new IOException(e);
    }
    
    // Create key value pairs for parsing
    lkey = (K1)this.leftRR.createKey();
    lvalue = (V1)this.leftRR.createValue();
    
    rkey = (K2)this.rightRR.createKey();
    rvalue = (V2)this.rightRR.createValue();
  }
  
  @Override
  public Text createKey() {
    return new Text();
  }
  
  @Override
  public Text createValue() {
    return new Text();
  }
  
  @Override
  public long getPos()
    throws IOException {
    return leftRR.getPos();
  }
  
  @Override
  public boolean next(Text key, Text value)
    throws IOException {
    do {
      StringBuilder v = new StringBuilder();
      v.append("[");
      
      if (goToNextLeft) {
        if (! leftRR.next(lkey, lvalue)) {
          alldone = true;
          break;
        } else {
          //key.set(lvalue.toString());
          v.append(lvalue.toString());
          
          goToNextLeft = alldone = false;
          
          this.rightRR = this.rightFIF.getRecordReader(this.rightIS,
                                                       this.rightConf,
                                                       this.rightReporter);
        }
      }
      
      if (rightRR.next(rkey, rvalue)) {
        //value.set(rvalue.toString());
        v.append(", ");
        v.append(rvalue.toString());
        v.append("]");
        
        key.set("null");
        value.set(v.toString());
      } else {
        goToNextLeft = true;
      }
    } while (goToNextLeft);
    
    return ! alldone;
  }
  
  @Override
  public void close()
    throws IOException {
    leftRR.close();
    rightRR.close();
  }
  
  @Override
  public float getProgress()
    throws IOException {
    return leftRR.getProgress();
  }
}

