// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <algorithm>
#include <condition_variable>
#include <map>
#include <mutex>

#include "common/global-types.h"
#include "gen-cpp/Types_types.h"
#include "util/container-util.h"

namespace impala {

class KrpcDataStreamRecvr;
class LocalRowBatchChannelManager;
class RowBatch;
class RuntimeState;

class LocalRowBatchChannel {
public:
  
  Status SendBatch(RowBatch* batch);

  void RegisterReceiver(KrpcDataStreamRecvr* receiver);
  void UnregisterSender();
  void UnregisterReceiver();
  void Cancel();

  void SendEos();

  void ReleaseInFlightBatch();

  Status WaitForInFlightBatch();

  int sender_id() const { return sender_id_; }
  RowBatch* in_flight_batch() const { return in_flight_batch_; }
private:
  friend class LocalRowBatchChannelManager;

  std::mutex lock_;

  RowBatch* in_flight_batch_ = nullptr;

  std::condition_variable_any* sender_cv_ = nullptr;
  std::condition_variable_any channel_cv_;
  KrpcDataStreamRecvr* receiver_ = nullptr;

  int sender_id_ = -1;

  bool sender_unregistered_ = false;
  bool receiver_unregistered_ = false;
  bool receiver_closed_status_returned_ = false;
  bool cancelled_ = false;
};

class LocalRowBatchChannelManager {
public:
  Status RegisterSender(RuntimeState* state, const PlanNodeId& receiver_node_id,
      const TUniqueId& receiver_instance_id, int sender_id,
      std::condition_variable_any* cv, LocalRowBatchChannel** channel);

  Status RegisterReceiver(const PlanNodeId& receiver_node_id,
      const TUniqueId& receiver_instance_id, KrpcDataStreamRecvr* receiver);

  void UnregisterReceiver(const PlanNodeId&  receiver_node_id,
      const TUniqueId& receiver_instance_id);

  void Cancel();

private:
  struct ReceiverRegistration {
    KrpcDataStreamRecvr* receiver_ = nullptr;
    std::vector<std::unique_ptr<LocalRowBatchChannel>> channels_;
    bool receiver_unregistered = false;
  };

  // If both taken, take sender_registration_lock_ first.
  std::mutex sender_registration_lock_;
  std::mutex receiver_registration_lock_;

  /// Node id and fragment instance id of receiver.
  typedef std::pair<PlanNodeId, TUniqueId> ReceiverId;
  typedef std::map<ReceiverId, ReceiverRegistration> ReceiverMap;
  ReceiverMap receiver_map_;

  bool prepare_finished_ = false;
};

}
