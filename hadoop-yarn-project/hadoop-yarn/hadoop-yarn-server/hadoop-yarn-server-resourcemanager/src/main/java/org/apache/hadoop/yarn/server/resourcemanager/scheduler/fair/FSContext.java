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

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.Clock;

import java.util.Timer;
import java.util.concurrent.PriorityBlockingQueue;

/**
 * Helper class that holds basic information to be passed around
 * FairScheduler classes.
 */
public class FSContext {
  private FairScheduler scheduler;
  private QueueManager queueManager;
  private Clock clock;

  // Preemption-related info
  private boolean preemptionEnabled;
  private float preemptionUtilizationThreshold;
  private long warnTimeBeforeKill;
  private Timer preemptionTimer = new Timer("Preemption Timer", true);
  private PriorityBlockingQueue<FSAppAttempt> starvedApps;

  public FairScheduler getScheduler() {
    return scheduler;
  }

  public void setScheduler(
      FairScheduler scheduler) {
    this.scheduler = scheduler;
  }

  public Resource getClusterResource() {
    return scheduler.getClusterResource();
  }

  public QueueManager getQueueManager() {
    return queueManager;
  }

  public void setQueueManager(
      QueueManager queueManager) {
    this.queueManager = queueManager;
  }

  public Clock getClock() {
    return clock;
  }

  public void setClock(Clock clock) {
    this.clock = clock;
  }

  public boolean isPreemptionEnabled() {
    return preemptionEnabled;
  }

  public void setPreemptionEnabled(boolean preemptionEnabled) {
    this.preemptionEnabled = preemptionEnabled;
  }

  public float getPreemptionUtilizationThreshold() {
    return preemptionUtilizationThreshold;
  }

  public void setPreemptionUtilizationThreshold(
      float preemptionUtilizationThreshold) {
    this.preemptionUtilizationThreshold = preemptionUtilizationThreshold;
  }

  public long getWarnTimeBeforeKill() {
    return warnTimeBeforeKill;
  }

  public void setWarnTimeBeforeKill(long warnTimeBeforeKill) {
    this.warnTimeBeforeKill = warnTimeBeforeKill;
  }

  public Timer getPreemptionTimer() {
    return this.preemptionTimer;
  }

  public PriorityBlockingQueue<FSAppAttempt> getStarvedApps() {
    return starvedApps;
  }

  public void setStarvedApps(
      PriorityBlockingQueue<FSAppAttempt> starvedApps) {
    this.starvedApps = starvedApps;
  }
}
