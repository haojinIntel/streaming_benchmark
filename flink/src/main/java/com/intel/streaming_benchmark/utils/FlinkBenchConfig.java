/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.intel.streaming_benchmark.utils;

import java.io.Serializable;

public class FlinkBenchConfig implements Serializable {
//  public String testCase;

  // Kafka related
  public String zkHost;
  public String brokerList;
  public String topic;
  public String consumerGroup;
//  public String offsetReset;
//  public String reportTopic;

  // Flink related
  public long checkpointDuration;
  public String resultLocation;
  public String sqlLocation;
  public String sqlName;
  public String timeType;

}
