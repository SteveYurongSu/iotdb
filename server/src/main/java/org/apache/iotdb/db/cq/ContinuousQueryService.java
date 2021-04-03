package org.apache.iotdb.db.cq;

import org.apache.iotdb.db.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.db.exception.ShutdownException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.qp.physical.sys.CreateContinuousQueryPlan;
import org.apache.iotdb.db.qp.physical.sys.DropContinuousQueryPlan;
import org.apache.iotdb.db.query.dataset.ShowContinuousQueriesResult;
import org.apache.iotdb.db.service.IService;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.service.ServiceType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public class ContinuousQueryService implements IService {

  private final HashMap<String, ScheduledFuture> continuousQueriesFutures = new HashMap<>();

  private final HashMap<String, CreateContinuousQueryPlan> continuousQueryPlans = new HashMap<>();

  private final ScheduledExecutorService pool =
      IoTDBThreadPoolFactory.newScheduledThreadPool(10, "Continuous Query Service");

  private final ReentrantLock registrationLock = new ReentrantLock();

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
  public void start() {}

  @Override
  public void stop() {}

  @Override
  public void waitAndStop(long milliseconds) {}

  @Override
  public void shutdown(long milliseconds) throws ShutdownException {}

  public boolean register(CreateContinuousQueryPlan plan, boolean writeLog) {

    if (writeLog) {
      try {
        IoTDB.metaManager.createContinuousQuery(plan);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    doRegister(plan);
    return true;
  }

  private void doRegister(CreateContinuousQueryPlan plan) {
    acquireRegistrationLock();

    try {

      ContinuousQuery cq = new ContinuousQuery(plan);
      ScheduledFuture future =
          pool.scheduleAtFixedRate(
              cq, plan.getEveryInterval(), plan.getEveryInterval(), TimeUnit.MILLISECONDS);
      continuousQueriesFutures.put(plan.getContinuousQueryName(), future);
      continuousQueryPlans.put(plan.getContinuousQueryName(), plan);
    } catch (QueryProcessException e) {
      e.printStackTrace();
    } finally {
      releaseRegistrationLock();
    }
  }

  public boolean deregister(DropContinuousQueryPlan plan) {

    try {

      IoTDB.metaManager.dropContinuousQuery(plan);
    } catch (Exception e) {

      e.printStackTrace();
    }

    doDeregister(plan);

    return true;
  }

  private void doDeregister(DropContinuousQueryPlan plan) {
    String cqName = plan.getContinuousQueryName();
    continuousQueriesFutures.get(cqName).cancel(false);
    continuousQueriesFutures.remove(cqName);
    continuousQueryPlans.remove(cqName);
  }

  public List<ShowContinuousQueriesResult> getContinuousQueryPlans() {

    List<ShowContinuousQueriesResult> results = new ArrayList<>(continuousQueryPlans.size());

    for (CreateContinuousQueryPlan plan : continuousQueryPlans.values()) {
      results.add(
          new ShowContinuousQueriesResult(
              plan.getQuerySql(),
              plan.getContinuousQueryName(),
              plan.getTargetPath(),
              plan.getEveryInterval(),
              plan.getForInterval()));
    }

    return results;
  }
}
