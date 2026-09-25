package it.unitn.ds;
import java.io.Serializable;
import java.lang.Comparable;

public class LogicalTimestamp implements Serializable, Comparable<LogicalTimestamp> {
  public final int epoch;
  public final int sequence_number;

  public LogicalTimestamp(int epoch, int sequence_number) {
    this.epoch = epoch;
    this.sequence_number = sequence_number;
  }
  
  @Override
  public int compareTo(LogicalTimestamp rhs) {
    int epoch_comparison = this.epoch - rhs.epoch;
    if(epoch_comparison == 0)
      return this.sequence_number - rhs.sequence_number;
    else
      return epoch_comparison;
  }
}
