package org.apache.iotdb.db.cq;

import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.qp.physical.sys.CreateContinuousQueryPlan;
import org.apache.iotdb.db.qp.physical.sys.DropContinuousQueryPlan;

import java.io.*;

public class ContinuousQueryLogWriter {

  //    public static final Byte REGISTER_TYPE = 0;
  //    public static final Byte DEREGISTER_TYPE = 1;

  private final File logFile;
  private final BufferedWriter writer;

  public ContinuousQueryLogWriter(String logFileName) throws IOException {
    logFile = SystemFileFactory.INSTANCE.getFile(logFileName);
    FileWriter fileWriter = new FileWriter(logFile, true);
    writer = new BufferedWriter(fileWriter);
  }

  public void close() throws IOException {
    writer.close();
  }

  public void deleteLogFile() throws IOException {
    if (!logFile.delete()) {
      throw new IOException("Failed to delete " + logFile + ".");
    }
  }

  public void register(CreateContinuousQueryPlan plan) throws IOException {

    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);

    try {
      plan.serialize(dataOutputStream);
    } catch (IOException e) {
      e.printStackTrace();
    }

    writer.write(byteArrayOutputStream.toByteArray().toString());

    writeLineAndFlush();
  }

  public void deregister(DropContinuousQueryPlan plan) throws IOException {

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(baos);
    oos.writeObject(plan);

    writer.write(baos.toString());
    writeLineAndFlush();
  }

  private void writeLineAndFlush() throws IOException {
    writer.newLine();
    writer.flush();
  }
}
