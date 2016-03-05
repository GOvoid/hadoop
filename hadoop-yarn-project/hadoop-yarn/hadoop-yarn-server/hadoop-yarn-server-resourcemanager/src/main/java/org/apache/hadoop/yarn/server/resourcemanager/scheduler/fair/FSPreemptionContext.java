package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerUtils;

import java.util.List;
import java.util.TimerTask;

/**
 * Captures the list of containers (all on one node) to be preempted to satisfy
 * the ResourceRequest of a starved app.
 */
class FSPreemptionContext {
  private static final Log LOG = LogFactory.getLog(FSPreemptionContext.class);

  private final FSContext context;
  private final List<RMContainer> containers;
  private TimerTask preemptContainersTask;

  public FSPreemptionContext(FSContext context, List<RMContainer> containers) {
    this.context = context;
    this.containers = containers;
    this.preemptContainersTask = new PreemptContainersTask();
  }

  public void preemptContainers() {
    // Warn application about containers to be killed
    for (RMContainer container : containers) {
      ApplicationAttemptId appAttemptId = container.getApplicationAttemptId();
      FSAppAttempt app = context.getScheduler().getSchedulerApp(appAttemptId);
      FSLeafQueue queue = app.getQueue();
      LOG.info("Preempting container " + container +
          " from queue " + queue.getName());
      app.addPreemption(container);
    }

    // Schedule timer task to kill containers
    context.getPreemptionTimer().schedule(
        preemptContainersTask, context.getWarnTimeBeforeKill());
  }

  class PreemptContainersTask extends TimerTask {
    @Override
    public void run() {
      for (RMContainer container : containers) {
        ContainerStatus status = SchedulerUtils.createPreemptedContainerStatus(
                container.getContainerId(), SchedulerUtils.PREEMPTED_CONTAINER);

        LOG.info("Killing container " + container);
        context.getScheduler().completedContainer(
            container, status, RMContainerEventType.KILL);
      }
    }
  }
}
