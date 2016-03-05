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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Thread that handles FairScheduler preemption
 */
public class FSPreemptionThread extends Thread {
  private static final Log LOG =
      LogFactory.getLog(FSPreemptionThread.class);
  private FSContext schedulerContext;

  public FSPreemptionThread(FSContext context) {
    this.schedulerContext = context;
    setDaemon(true);
    setName("FairScheduler Preemption Thread");
  }

  @Override
  public void run() {
    while (!Thread.interrupted()) {
      FSAppAttempt starvedApp;

      try{
        starvedApp = schedulerContext.getStarvedApps().take();
      } catch (InterruptedException e) {
        LOG.info("Preemption thread interrupted! Exiting.");
        return;
      }

      FSPreemptionContext preemptionContext =
          identifyContainersToPreempt(starvedApp);
      if (preemptionContext != null) {
        preemptionContext.preemptContainers();
      }
    }
  }

  /**
   * Returns a non-null PremptionContext if it finds a node that can
   * accommodate a request from this app. Also, reserves the node for this app.
   */
  private FSPreemptionContext identifyContainersToPreempt(FSAppAttempt starvedApp) {
    /**
     * TODO:
     * 1. Find a set of nodes for the highest priority ResourceRequest
     * 2. Iterate through nodes until we find a node with enough resources
     * from applications over their fairshare to meet the ResourceRequest.
     * 3. Reserve the node for this app to finalize the node and the
     * containers before returning.
     */
    return null;
  }

}
