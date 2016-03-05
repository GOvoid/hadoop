/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.TreeSet;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.resource.ResourceWeights;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ActiveUsersManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerAppUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.policies.DominantResourceFairnessPolicy;
import org.apache.hadoop.yarn.util.resource.Resources;

@Private
@Unstable
public class FSLeafQueue extends FSQueue {
  private static final Log LOG = LogFactory.getLog(
      FSLeafQueue.class.getName());

  // apps that are runnable
  private final List<FSAppAttempt> runnableApps = new ArrayList<>();
  private final List<FSAppAttempt> nonRunnableApps = new ArrayList<>();
  // get a lock with fair distribution for app list updates
  private final ReadWriteLock rwl = new ReentrantReadWriteLock(true);
  private final Lock readLock = rwl.readLock();
  private final Lock writeLock = rwl.writeLock();
  
  private Resource demand = Resources.createResource(0);
  
  // Variables used for preemption
  private long lastTimeAtMinShare;

  // Track the AM resource usage for this queue
  private Resource amResourceUsage;

  private final ActiveUsersManager activeUsersManager;
  public static final List<FSQueue> EMPTY_LIST = Collections.emptyList();

  public FSLeafQueue(FSContext context, FSParentQueue parent, String name) {
    super(context, parent, name);
    this.lastTimeAtMinShare = context.getClock().getTime();
    activeUsersManager = new ActiveUsersManager(getMetrics());
    amResourceUsage = Resource.newInstance(0, 0);
  }
  
  public void addApp(FSAppAttempt app, boolean runnable) {
    writeLock.lock();
    try {
      if (runnable) {
        runnableApps.add(app);
      } else {
        nonRunnableApps.add(app);
      }
    } finally {
      writeLock.unlock();
    }
  }
  
  // for testing
  void addAppSchedulable(FSAppAttempt appSched) {
    writeLock.lock();
    try {
      runnableApps.add(appSched);
    } finally {
      writeLock.unlock();
    }
  }
  
  /**
   * Removes the given app from this queue.
   * @return whether or not the app was runnable
   */
  public boolean removeApp(FSAppAttempt app) {
    boolean runnable = false;

    // Remove app from runnable/nonRunnable list while holding the write lock
    writeLock.lock();
    try {
      runnable = runnableApps.remove(app);
      if (!runnable) {
        // removeNonRunnableApp acquires the write lock again, which is fine
        if (!removeNonRunnableApp(app)) {
          throw new IllegalStateException("Given app to remove " + app +
              " does not exist in queue " + this);
        }
      }
    } finally {
      writeLock.unlock();
    }

    // Update AM resource usage if needed. If isAMRunning is true, we're not
    // running an unmanaged AM.
    if (runnable && app.isAmRunning()) {
      Resources.subtractFrom(amResourceUsage, app.getAMResource());
    }

    return runnable;
  }

  /**
   * Removes the given app if it is non-runnable and belongs to this queue
   * @return true if the app is removed, false otherwise
   */
  public boolean removeNonRunnableApp(FSAppAttempt app) {
    writeLock.lock();
    try {
      return nonRunnableApps.remove(app);
    } finally {
      writeLock.unlock();
    }
  }

  public boolean isRunnableApp(FSAppAttempt attempt) {
    readLock.lock();
    try {
      return runnableApps.contains(attempt);
    } finally {
      readLock.unlock();
    }
  }

  public boolean isNonRunnableApp(FSAppAttempt attempt) {
    readLock.lock();
    try {
      return nonRunnableApps.contains(attempt);
    } finally {
      readLock.unlock();
    }
  }

  public void resetPreemptedResources() {
    readLock.lock();
    try {
      for (FSAppAttempt attempt : runnableApps) {
        attempt.resetPreemptedResources();
      }
    } finally {
      readLock.unlock();
    }
  }

  public void clearPreemptedResources() {
    readLock.lock();
    try {
      for (FSAppAttempt attempt : runnableApps) {
        attempt.clearPreemptedResources();
      }
    } finally {
      readLock.unlock();
    }
  }

  public List<FSAppAttempt> getCopyOfNonRunnableAppSchedulables() {
    List<FSAppAttempt> appsToReturn = new ArrayList<FSAppAttempt>();
    readLock.lock();
    try {
      appsToReturn.addAll(nonRunnableApps);
    } finally {
      readLock.unlock();
    }
    return appsToReturn;
  }

  @Override
  public void collectSchedulerApplications(
      Collection<ApplicationAttemptId> apps) {
    readLock.lock();
    try {
      for (FSAppAttempt appSched : runnableApps) {
        apps.add(appSched.getApplicationAttemptId());
      }
      for (FSAppAttempt appSched : nonRunnableApps) {
        apps.add(appSched.getApplicationAttemptId());
      }
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public void setPolicy(SchedulingPolicy policy)
      throws AllocationConfigurationException {
    if (!SchedulingPolicy.isApplicableTo(policy, SchedulingPolicy.DEPTH_LEAF)) {
      throwPolicyDoesnotApplyException(policy);
    }
    super.policy = policy;
  }

  @Override
  public void updateInternal(boolean checkStarvation) {
    readLock.lock();
    try {
      policy.computeShares(runnableApps, getFairShare());
      if (checkStarvation) {
        identifyStarvedApplications();
      }
    } finally {
      readLock.unlock();
    }
  }

  /**
   * Helper method to identify starved applications. This needs to be called
   * ONLY from {@link #updateInternal(boolean)}, after the application shares
   * are updated.
   *
   * A queue can be starving due to fairshare or minshare.
   *
   * Minshare is defined only on the queue and not the applications.
   * Fairshare is defined for both the queue and the applications.
   *
   * If this queue is starved due to minshare, we need to identify the most
   * deserving apps if they themselves are not starved due to fairshare.
   *
   * If this queue is starving due to fairshare, there must be at least
   * one application that is starved. And, even if the queue is not
   * starved due to fairshare, there might still be starved applications.
   */
  private void identifyStarvedApplications() {
    // First identify starved applications and track total amount of
    // starvation (in resources)
    Resource fairShareStarvation = Resources.clone(Resources.none());

    TreeSet<FSAppAttempt> appsWithDemand = fetchAppsWithDemand();
    for (FSAppAttempt app : appsWithDemand) {
      // TODO (KK): Starvation should be based on threshold and timeout
      Resource appStarvation = app.fairShareStarvation();
      if (Resources.equals(Resources.none(), appStarvation))  {
        break;
      } else {
        context.getStarvedApps().add(app);
        Resources.addTo(fairShareStarvation, appStarvation);
      }
    }

    // Compute extent of minshare starvation
    Resource minShareStarvation = minShareStarvation();

    // Compute minshare starvation that is not subsumed by fairshare starvation
    Resources.subtractFrom(minShareStarvation, fairShareStarvation);

    // Keep adding apps to the starved list until the unmet demand goes over
    // the remaining minshare
    for (FSAppAttempt app : appsWithDemand) {
      if (Resources.greaterThan(policy.getResourceCalculator(),
          context.getClusterResource(), minShareStarvation, Resources.none())) {

        context.getStarvedApps().add(app);
        Resources.subtractFrom(minShareStarvation,
            Resources.subtract(app.getDemand(), app.getResourceUsage()));
      }
    }
  }

  @Override
  public Resource getDemand() {
    return demand;
  }

  @Override
  public Resource getResourceUsage() {
    Resource usage = Resources.createResource(0);
    readLock.lock();
    try {
      for (FSAppAttempt app : runnableApps) {
        Resources.addTo(usage, app.getResourceUsage());
      }
      for (FSAppAttempt app : nonRunnableApps) {
        Resources.addTo(usage, app.getResourceUsage());
      }
    } finally {
      readLock.unlock();
    }
    return usage;
  }

  public Resource getAmResourceUsage() {
    return amResourceUsage;
  }

  @Override
  public void updateDemand() {
    // Compute demand by iterating through apps in the queue
    // Limit demand to maxResources
    Resource maxRes = scheduler.getAllocationConfiguration()
        .getMaxResources(getName());
    demand = Resources.createResource(0);
    readLock.lock();
    try {
      for (FSAppAttempt sched : runnableApps) {
        if (Resources.equals(demand, maxRes)) {
          break;
        }
        updateDemandForApp(sched, maxRes);
      }
      for (FSAppAttempt sched : nonRunnableApps) {
        if (Resources.equals(demand, maxRes)) {
          break;
        }
        updateDemandForApp(sched, maxRes);
      }
    } finally {
      readLock.unlock();
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("The updated demand for " + getName() + " is " + demand
          + "; the max is " + maxRes);
      LOG.debug("The updated fairshare for " + getName() + " is "
          + getFairShare());
    }
  }
  
  private void updateDemandForApp(FSAppAttempt sched, Resource maxRes) {
    sched.updateDemand();
    Resource toAdd = sched.getDemand();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Counting resource from " + sched.getName() + " " + toAdd
          + "; Total resource consumption for " + getName() + " now "
          + demand);
    }
    demand = Resources.add(demand, toAdd);
    demand = Resources.componentwiseMin(demand, maxRes);
  }

  @Override
  public Resource assignContainer(FSSchedulerNode node) {
    Resource assigned = Resources.none();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Node " + node.getNodeName() + " offered to queue: " +
          getName() + " fairShare: " + getFairShare());
    }

    if (!assignContainerPreCheck(node)) {
      return assigned;
    }

    for (FSAppAttempt sched : fetchAppsWithDemand()) {
      if (SchedulerAppUtils.isBlacklisted(sched, node, LOG)) {
        continue;
      }
      assigned = sched.assignContainer(node);
      if (!assigned.equals(Resources.none())) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Assigned container in queue:" + getName() + " " +
              "container:" + assigned);
        }
        break;
      }
    }
    return assigned;
  }

  private TreeSet<FSAppAttempt> fetchAppsWithDemand() {
    TreeSet<FSAppAttempt> pendingForResourceApps =
        new TreeSet<>(policy.getComparator());
    readLock.lock();
    try {
      for (FSAppAttempt app : runnableApps) {
        Resource pending = app.getAppAttemptResourceUsage().getPending();
        if (!pending.equals(Resources.none())) {
          pendingForResourceApps.add(app);
        }
      }
    } finally {
      readLock.unlock();
    }
    return pendingForResourceApps;
  }

  @Override
  public RMContainer preemptContainer() {
    RMContainer toBePreempted = null;

    // If this queue is not over its fair share, reject
    if (!preemptContainerPreCheck()) {
      return toBePreempted;
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Queue " + getName() + " is going to preempt a container " +
          "from its applications.");
    }

    // Choose the app that is most over fair share
    Comparator<Schedulable> comparator = policy.getComparator();
    FSAppAttempt candidateSched = null;
    readLock.lock();
    try {
      for (FSAppAttempt sched : runnableApps) {
        if (candidateSched == null ||
            comparator.compare(sched, candidateSched) > 0) {
          candidateSched = sched;
        }
      }
    } finally {
      readLock.unlock();
    }

    // Preempt from the selected app
    if (candidateSched != null) {
      toBePreempted = candidateSched.preemptContainer();
    }
    return toBePreempted;
  }

  @Override
  public List<FSQueue> getChildQueues() {
    return EMPTY_LIST;
  }
  
  @Override
  public List<QueueUserACLInfo> getQueueUserAclInfo(UserGroupInformation user) {
    QueueUserACLInfo userAclInfo =
      recordFactory.newRecordInstance(QueueUserACLInfo.class);
    List<QueueACL> operations = new ArrayList<QueueACL>();
    for (QueueACL operation : QueueACL.values()) {
      if (hasAccess(operation, user)) {
        operations.add(operation);
      }
    }

    userAclInfo.setQueueName(getQueueName());
    userAclInfo.setUserAcls(operations);
    return Collections.singletonList(userAclInfo);
  }
  
  public long getLastTimeAtMinShare() {
    return lastTimeAtMinShare;
  }

  private void setLastTimeAtMinShare(long lastTimeAtMinShare) {
    this.lastTimeAtMinShare = lastTimeAtMinShare;
  }

  @Override
  public int getNumRunnableApps() {
    readLock.lock();
    try {
      return runnableApps.size();
    } finally {
      readLock.unlock();
    }
  }

  public int getNumNonRunnableApps() {
    readLock.lock();
    try {
      return nonRunnableApps.size();
    } finally {
      readLock.unlock();
    }
  }

  public int getNumPendingApps() {
    int numPendingApps = 0;
    readLock.lock();
    try {
      for (FSAppAttempt attempt : runnableApps) {
        if (attempt.isPending()) {
          numPendingApps++;
        }
      }
      numPendingApps += nonRunnableApps.size();
    } finally {
      readLock.unlock();
    }
    return numPendingApps;
  }

  /**
   * TODO: Based on how frequently this is called, we might want to club
   * counting pending and active apps in the same method.
   */
  public int getNumActiveApps() {
    int numActiveApps = 0;
    readLock.lock();
    try {
      for (FSAppAttempt attempt : runnableApps) {
        if (!attempt.isPending()) {
          numActiveApps++;
        }
      }
    } finally {
      readLock.unlock();
    }
    return numActiveApps;
  }

  @Override
  public ActiveUsersManager getActiveUsersManager() {
    return activeUsersManager;
  }

  /**
   * Check whether this queue can run this application master under the
   * maxAMShare limit. For FIFO and FAIR policies, check if the VCore usage
   * takes up the entire cluster or maxResources for the queue.
   * @param amResource
   * @return true if this queue can run
   */
  public boolean canRunAppAM(Resource amResource) {
    float maxAMShare =
        scheduler.getAllocationConfiguration().getQueueMaxAMShare(getName());
    if (Math.abs(maxAMShare - -1.0f) < 0.0001) {
      return true;
    }
    Resource maxAMResource = Resources.multiply(getFairShare(), maxAMShare);
    Resource ifRunAMResource = Resources.add(amResourceUsage, amResource);

    boolean overMaxAMShareLimit = policy
            .checkIfAMResourceUsageOverLimit(ifRunAMResource, maxAMResource);

    // For fair policy and fifo policy which doesn't check VCore usages,
    // additionally check if the AM takes all available VCores or
    // over maxResource to avoid deadlock.
    if (!overMaxAMShareLimit && !policy.equals(
        SchedulingPolicy.getInstance(DominantResourceFairnessPolicy.class))) {
      overMaxAMShareLimit =
         isVCoresOverMaxResource(ifRunAMResource.getVirtualCores()) ||
         ifRunAMResource.getVirtualCores() >=
         scheduler.getRootQueueMetrics().getAvailableVirtualCores();
    }

    return !overMaxAMShareLimit;
  }

  public void addAMResourceUsage(Resource amResource) {
    if (amResource != null) {
      Resources.addTo(amResourceUsage, amResource);
    }
  }

  @Override
  public void recoverContainer(Resource clusterResource,
      SchedulerApplicationAttempt schedulerAttempt, RMContainer rmContainer) {
    // TODO Auto-generated method stub
  }

  /** Allows setting weight for a dynamically created queue
   * Currently only used for reservation based queues
   * @param weight queue weight
   */
  public void setWeights(float weight) {
    scheduler.getAllocationConfiguration().setQueueWeight(getName(),
        new ResourceWeights(weight));
  }

  /**
   * Helper method to check if the queue should preempt containers
   *
   * @return true if check passes (can preempt) or false otherwise
   */
  private boolean preemptContainerPreCheck() {
    return parent.getPolicy().checkIfUsageOverFairShare(getResourceUsage(),
        getFairShare());
  }

  private Resource minShareStarvation() {
    Resource desiredShare = Resources.min(policy.getResourceCalculator(),
        scheduler.getClusterResource(), getMinShare(), getDemand());

    Resource starvation = Resources.subtract(desiredShare, getResourceUsage());
    boolean starved = Resources.greaterThan(policy.getResourceCalculator(),
        scheduler.getClusterResource(), starvation, Resources.none());

    long now = context.getClock().getTime();
    if (!starved) {
      setLastTimeAtMinShare(now);
    }

    if (starved &&
        (now - lastTimeAtMinShare > getMinSharePreemptionTimeout())) {
      return starvation;
    } else {
      return Resources.none();
    }
  }
}
