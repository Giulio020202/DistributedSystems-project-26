package it.unitn.ds;
import java.io.Serializable;
import java.lang.Comparable;
import java.util.Objects;

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

  @Override
  public boolean equals(Object rhs) {
    // These ifs are not very efficient and it would be better to
    // cramp everything into a single boolean expression,
    // but it would be unreadable
    if(this == rhs)
      return true;
    if(rhs == null || !(rhs instanceof LogicalTimestamp))
      return false;
    LogicalTimestamp rhs_timestamp = (LogicalTimestamp) rhs;
    return this.epoch == rhs_timestamp.epoch && this.sequence_number == rhs_timestamp.sequence_number;
  }

  @Override
  public int hashCode() {
    return Objects.hash(epoch, sequence_number);
  }
}
