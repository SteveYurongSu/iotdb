package org.apache.iotdb.db.cq;

import org.apache.commons.io.FileUtils;
import org.apache.iotdb.db.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.exception.ShutdownException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateContinuousQueryPlan;
import org.apache.iotdb.db.qp.physical.sys.DropContinuousQueryPlan;
import org.apache.iotdb.db.query.dataset.ShowContinuousQueriesResult;
import org.apache.iotdb.db.query.udf.service.UDFLogWriter;
import org.apache.iotdb.db.service.IService;
import org.apache.iotdb.db.service.ServiceType;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ContinuousQueryService implements IService {

  private static final String CQ_LOG_FILE_DIR =
      IoTDBDescriptor.getInstance().getConfig().getSystemDir()
          + File.separator
          + "cq"
          + File.separator;
  private static final String CQ_LOG_FILE_NAME = CQ_LOG_FILE_DIR + "log.txt";
  private static final String CQ_TEMPORARY_LOG_FILE_NAME = CQ_LOG_FILE_NAME + ".tmp";

  private final HashMap<String, ScheduledFuture> continuousQueriesFutures = new HashMap<>();

  private final HashMap<String, CreateContinuousQueryPlan> continuousQueryPlans = new HashMap<>();

  private final ScheduledExecutorService pool =
      IoTDBThreadPoolFactory.newScheduledThreadPool(10, "Continuous Query Service");

  private final ReentrantLock registrationLock = new ReentrantLock();
  private final ReentrantReadWriteLock logWriterLock = new ReentrantReadWriteLock();
  private ContinuousQueryLogWriter logWriter;

  private static final ContinuousQueryService INSTANCE = new ContinuousQueryService();

  public ContinuousQueryService() {}

  public static ContinuousQueryService getInstance() {
    return INSTANCE;
  }

  public void acquireRegistrationLock() {
    registrationLock.lock();
  }

  public void releaseRegistrationLock() {
    registrationLock.unlock();
  }

  @Override
  public ServiceType getID() {
    return ServiceType.CONTINUOUS_QUERY_SERVICE;
  }

  @Override
  public void start() {
    try {
      makeDirIfNecessary();
      doRecovery();
      logWriter = new ContinuousQueryLogWriter(CQ_LOG_FILE_NAME);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Override
  public void stop() {
    try {
      writeTemporaryLogFile();

      logWriter.close();
      logWriter.deleteLogFile();

      File temporaryLogFile = SystemFileFactory.INSTANCE.getFile(CQ_TEMPORARY_LOG_FILE_NAME);
      File logFile = SystemFileFactory.INSTANCE.getFile(CQ_LOG_FILE_NAME);
      FSFactoryProducer.getFSFactory().moveFile(temporaryLogFile, logFile);
    } catch (IOException ignored) {
      // ignored
    }
  }

  private void writeTemporaryLogFile() throws IOException {
    ContinuousQueryLogWriter temporaryLogFile =
        new ContinuousQueryLogWriter(CQ_TEMPORARY_LOG_FILE_NAME);
    for (CreateContinuousQueryPlan plan : continuousQueryPlans.values()) {
      temporaryLogFile.register(plan);
    }
    temporaryLogFile.close();
  }

  @Override
  public void waitAndStop(long milliseconds) {}

  @Override
  public void shutdown(long milliseconds) throws ShutdownException {}

  public boolean register(CreateContinuousQueryPlan plan) {
    try {
      appendRegistrationLog(plan);
    } catch (IOException e) {
      e.printStackTrace();
    }

    doRegister(plan);
    return true;
  }

  private void doRegister(CreateContinuousQueryPlan plan) {
    acquireRegistrationLock();

    try {

      ContinuousQuery cq = new ContinuousQuery(plan);
      pool.scheduleAtFixedRate(
          cq, plan.getEveryInterval(), plan.getEveryInterval(), TimeUnit.MILLISECONDS);
      continuousQueryPlans.put(plan.getContinuousQueryName(), plan);
    } catch (QueryProcessException e) {
      e.printStackTrace();
    } finally {
      releaseRegistrationLock();
    }
  }

  public boolean deregister(DropContinuousQueryPlan plan) {

    try {

      appendDeregistrationLog(plan);
    } catch (Exception e) {

      e.printStackTrace();
    }

    String cqName = plan.getContinuousQueryName();
    continuousQueriesFutures.get(cqName).cancel(false);
    continuousQueriesFutures.remove(cqName);
    CreateContinuousQueryPlan originalPlan = continuousQueryPlans.remove(cqName);

    return true;
  }

  private void appendRegistrationLog(CreateContinuousQueryPlan plan) throws IOException {
    logWriterLock.writeLock().lock();
    try {
      logWriter.register(plan);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      logWriterLock.writeLock().unlock();
    }
  }

  private void appendDeregistrationLog(DropContinuousQueryPlan plan) throws IOException {
    logWriterLock.writeLock().lock();
    try {
      logWriter.deregister(plan);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      logWriterLock.writeLock().unlock();
    }
  }

  public List<ShowContinuousQueriesResult> getContinuousQueryPlans() {

    return null;
  }

  private void makeDirIfNecessary() throws IOException {
    File file = SystemFileFactory.INSTANCE.getFile(CQ_LOG_FILE_DIR);
    if (file.exists() && file.isDirectory()) {
      return;
    }
    FileUtils.forceMkdir(file);
  }

  private void doRecovery() {
    File temporaryLogFile = SystemFileFactory.INSTANCE.getFile(CQ_TEMPORARY_LOG_FILE_NAME);
    File logFile = SystemFileFactory.INSTANCE.getFile(CQ_LOG_FILE_NAME);
    try {
      if (temporaryLogFile.exists()) {
        if (logFile.exists()) {
          recoveryFromLogFile(logFile);
          FileUtils.deleteQuietly(temporaryLogFile);
        } else {
          recoveryFromLogFile(temporaryLogFile);
          FSFactoryProducer.getFSFactory().moveFile(temporaryLogFile, logFile);
        }
      } else if (logFile.exists()) {
        recoveryFromLogFile(logFile);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private void recoveryFromLogFile(File logFile) throws IOException {
    HashMap<String, CreateContinuousQueryPlan> recoveredCQs = new HashMap<>();

    try (BufferedReader reader = new BufferedReader(new FileReader(logFile))) {
      String line;
      while ((line = reader.readLine()) != null) {
//        ByteArrayInputStream bais = new ByteArrayInputStream(line.getBytes());
//        ObjectInputStream ois = new ObjectInputStream(bais);

        PhysicalPlan plan = PhysicalPlan.Factory.create(ByteBuffer.wrap(line.getBytes()));

//        Object plan = ois.readObject();
        if (plan instanceof CreateContinuousQueryPlan) {
          recoveredCQs.put(
              ((CreateContinuousQueryPlan) plan).getContinuousQueryName(),
              (CreateContinuousQueryPlan) plan);
        } else if (plan instanceof DropContinuousQueryPlan) {
          recoveredCQs.remove(((DropContinuousQueryPlan) plan).getContinuousQueryName());
        }
      }
    } catch ( IllegalPathException e) {
      e.printStackTrace();
    }

    for (Map.Entry<String, CreateContinuousQueryPlan> cq : recoveredCQs.entrySet()) {
      register(cq.getValue());
    }
  }
}
