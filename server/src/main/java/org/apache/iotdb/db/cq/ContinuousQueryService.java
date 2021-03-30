package org.apache.iotdb.db.cq;

import org.apache.iotdb.db.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.db.exception.ShutdownException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.physical.sys.CreateContinuousQueryPlan;
import org.apache.iotdb.db.qp.physical.sys.DropContinuousQueryPlan;
import org.apache.iotdb.db.query.dataset.ShowContinuousQueriesResult;
import org.apache.iotdb.db.service.IService;
import org.apache.iotdb.db.service.ServiceType;

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class ContinuousQueryService implements IService {

  private final HashMap<String, ScheduledFuture> continuousQueriesFutures = new HashMap<>();

  private final HashMap<String, ContinuousQuery> continuousQueries = new HashMap<>();

  private final ScheduledExecutorService pool =
      IoTDBThreadPoolFactory.newScheduledThreadPool(10, "Continuous Query Service");

  private static final ContinuousQueryService INSTANCE = new ContinuousQueryService();

  public ContinuousQueryService() {}

  public static ContinuousQueryService getInstance() {
    return INSTANCE;
  }

  @Override
  public ServiceType getID() {
    return ServiceType.CONTINUOUS_QUERY_SERVICE;
  }

  @Override
  public void start() {
    recover();
  }

  @Override
  public void stop() {}

  @Override
  public void waitAndStop(long milliseconds) {}

  @Override
  public void shutdown(long milliseconds) throws ShutdownException {}

  public boolean register(CreateContinuousQueryPlan plan) {

    // construct CQ
    ContinuousQuery cq = null;
    try {
      cq = new ContinuousQuery(plan);
    } catch (IllegalPathException e) {
      e.printStackTrace();
    } catch (QueryProcessException e) {
      e.printStackTrace();
    }

    // write operation info and serielize plan into log
    //        writeLog("CREATE " + cqName);
    //        writeLog(plan.serielize());

    // put into pool
    pool.scheduleAtFixedRate(
        cq,
        plan.getEveryInterval(),
        plan.getEveryInterval(),
        TimeUnit.MILLISECONDS);

    return true;
  }

  public boolean deregister(DropContinuousQueryPlan plan) {
    // remove CQ
    String cqName = plan.getContinuousQueryName();
    continuousQueriesFutures.get(cqName).cancel(false);
    continuousQueriesFutures.remove(cqName);
    continuousQueries.remove(cqName);

    // write operation info into log
    // writeLog(plan.serielize());

    return true;
  }

  public List<ShowContinuousQueriesResult> getContinuousQueries() {

    return null;
  }

  private void recover() {
    // read cq info from log
    //        while (!EOF) {
    //            PhysicalPlan plan = readNext().deserielize();
    //            if (plan instanceof CreateContinuousQueryPlan) {
    //                continuousQueries.put(cqname, plan);
    //            }
    //            else if (plan instanceof DropContinuousQueryPlan) {
    //                continuousQueries.remove(cqname);
    //            }
    //        }
    //
    //        // clear log
    //        clearLog();
    //
    //        // recover cqs and write cq info into log
    //        for (pair in continuousQueries) {
    //            register(pair.second);
    //        }

  }
}
