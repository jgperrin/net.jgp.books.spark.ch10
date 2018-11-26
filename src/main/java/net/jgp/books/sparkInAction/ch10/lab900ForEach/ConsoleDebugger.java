package net.jgp.books.sparkInAction.ch10.lab900ForEach;

import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;

public class ConsoleDebugger extends ForeachWriter<Row> {
  private static final long serialVersionUID = 4137020658417523102L;

  @Override
  public void close(Throwable arg0) {
    // TODO Auto-generated method stub

  }

  @Override
  public boolean open(long arg0, long arg1) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public void process(Row arg0) {
    

  }

}
