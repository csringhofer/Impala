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

#include "runtime/local-row-batch-channel.h"

#include "runtime/krpc-data-stream-recvr.h"
#include "runtime/query-state.h"
#include "runtime/runtime-state.h"

using namespace std;

namespace impala {

Status LocalRowBatchChannel::SendBatch(RowBatch* batch) {
  {
    //LOG(INFO) << "LocalRowBatchChannel::SendBatch";
    lock_guard<mutex> l(lock_);
    // Receiver is registered during prepare which must have finisged for all
    // fragment instances for this query at this point.
    DCHECK_NE(receiver_, nullptr);
    DCHECK(!sender_unregistered_);
    // NOOP if the receiver already unregistered.
    if (receiver_unregistered_ ) return Status::OK();
    if (cancelled_) return Status::CANCELLED;

    while (in_flight_batch_ != nullptr) {
      // Wait for:
      //    the batch to be consumed
      // or the receiver_ to registared
      // or the channel to be closed
      //LOG(INFO) << "LocalRowBatchChannel::SendBatch wait channel_cv_";
      channel_cv_.wait(lock_);
      if (receiver_unregistered_) return Status::OK();
      if (cancelled_) return Status::CANCELLED;
    }
    
    DCHECK_EQ(in_flight_batch_, nullptr);
    // Set in_flight_batch_ before releasing the lock as ReleaseInFlightBatch() can be
    // called between AddLocalBatch() and taking the lock again.
    in_flight_batch_ = batch;
  }
  // Release the lock to avoid deadlocks with the receiver channel's lock.
  bool deferred_by_receiver = false;
  //LOG(INFO) << "LocalRowBatchChannel::SendBatch RPC start";
  Status status = receiver_->AddLocalBatch(batch, this, &deferred_by_receiver);
  if (!deferred_by_receiver) {
      lock_guard<mutex> l(lock_);
      in_flight_batch_ = nullptr;
      // It is possible that in_flight_batch_ is already nullptr at this point if
      // ReleaseInFlightBatch() was called. This is not a problem.
      if (status.code() == TErrorCode::DATASTREAM_RECVR_CLOSED) {
        // This means that the reciever was cancelled
        // TODO: ideally the receiver would always unregister by this point.
        //DCHECK(receiver_unregistered_);
        DCHECK(!receiver_closed_status_returned_);
        receiver_closed_status_returned_ = true;
        receiver_unregistered_ = true;
        status = Status::OK();
      }
  }
  //LOG(INFO) << "LocalRowBatchChannel::SendBatch RPC finished";
  // TODO: return error if cancelled
  return status;
}

void LocalRowBatchChannel::ReleaseInFlightBatch() {
  //LOG(INFO) << "LocalRowBatchChannel::ReleaseInFlightBatch()";
  lock_guard<mutex> l(lock_);
  DCHECK_NE(receiver_, nullptr);

  if (receiver_unregistered_) {
    // Responding to remaining deferred RPCs after the receiver is unregistered.
    // UnregisterReceiver has already set in_flight_batch_ to nullptr;
    DCHECK_EQ(in_flight_batch_, nullptr);
  } else {
    DCHECK_NE(in_flight_batch_, nullptr);
    in_flight_batch_ = nullptr;
    channel_cv_.notify_all();
  }  
}


Status LocalRowBatchChannel::WaitForInFlightBatch() {
  //LOG(INFO) << "LocalRowBatchChannel::WaitForInFlightBatch()";
  lock_guard<mutex> l(lock_);
  DCHECK(!sender_unregistered_);
  // NOOP if the receiver already unregistered.
  if (receiver_unregistered_) return Status::OK();
  if (cancelled_) return Status::CANCELLED;

  while (in_flight_batch_ != nullptr) {
    // Wait for:
    //    the batch to be consumed
    // or the receiver_ to registared
    // or the channel to be closed
    //LOG(INFO) << "LocalRowBatchChannel::SendBatch wait channel_cv_";
    channel_cv_.wait(lock_);
    if (receiver_unregistered_) return Status::OK();
    if (cancelled_) return Status::CANCELLED;
  }
  
  DCHECK_NE(receiver_, nullptr);
  DCHECK_EQ(in_flight_batch_, nullptr);
  //LOG(INFO) << "LocalRowBatchChannel::WaitForInFlightBatch() finished";
  return Status::OK();
}


void LocalRowBatchChannel::RegisterReceiver(KrpcDataStreamRecvr* receiver) {
  lock_guard<mutex> l(lock_);
  DCHECK_EQ(receiver_, nullptr);
  receiver_ = receiver;
  channel_cv_.notify_all();
    // TODO: return error if cancelled?
}

void LocalRowBatchChannel::UnregisterSender() {
  //LOG(INFO) << "LocalRowBatchChannel::UnregisterSender";
  lock_guard<mutex> l(lock_);
  while (in_flight_batch_ != nullptr) {
    // Wait for:
    //    the batch to be consumed
    // or the channel to be closed
    channel_cv_.wait(lock_);
    if (receiver_unregistered_ || cancelled_) return;
  }
  sender_unregistered_ = true;
  //LOG(INFO) << "LocalRowBatchChannel::UnregisterSender finished";
}

void LocalRowBatchChannel::UnregisterReceiver() {
  //LOG(INFO) << "LocalRowBatchChannel::UnregisterReceiver";
  lock_guard<mutex> l(lock_);
  DCHECK_NE(receiver_, nullptr);
  if (!receiver_closed_status_returned_) DCHECK(!receiver_unregistered_);
  receiver_unregistered_ = true;
  // Unblock threads that wait for in-flight batch to be consumed.
  in_flight_batch_ = nullptr;
  channel_cv_.notify_all();
}

void LocalRowBatchChannel::Cancel() {
  //LOG(INFO) << "LocalRowBatchChannel::Cancel";
  lock_guard<mutex> l(lock_);
  DCHECK(!cancelled_);
  cancelled_ = true;
  channel_cv_.notify_all();
}

void LocalRowBatchChannel::SendEos() {
  {
    //LOG(INFO) << "LocalRowBatchChannel::SendEos";
    lock_guard<mutex> l(lock_);
    //LOG(INFO) << "LocalRowBatchChannel::SendEos lock taken";
    DCHECK(!sender_unregistered_);
    DCHECK_NE(receiver_, nullptr);
    // NOOP if the receiver already unregistered.
    if (receiver_unregistered_ || cancelled_) return;

    while (in_flight_batch_ != nullptr) {
      // Wait for:
      //    the batch to be consumed
      // or the receiver_ to registared
      // or the channel to be closed
      channel_cv_.wait(lock_);
      if (receiver_unregistered_ || cancelled_) return;
    }
    

    DCHECK_EQ(in_flight_batch_, nullptr);
  }
  //LOG(INFO) << "LocalRowBatchChannel::SendEos RemoveSender";
  receiver_->RemoveSender(sender_id_);
  //LOG(INFO) << "LocalRowBatchChannel::SendEos RemoveSender finished";
}

Status LocalRowBatchChannelManager::RegisterSender(RuntimeState* state, 
    const PlanNodeId& receiver_node_id, const TUniqueId& receiver_instance_id,
    int sender_id, std::condition_variable_any* cv, LocalRowBatchChannel** channel_out) {
  lock_guard<mutex> l1(sender_registration_lock_);
  if (!prepare_finished_) {
    // Wait for prepare to be finished to ensure that receivers are registered.
    // Release lock to allow registration of receivers.
    // TODO: this is probable not really needed, could access WaitForPrepare() directly.
    RETURN_IF_ERROR(state->query_state()->WaitForPrepare());
    prepare_finished_ = true;
  }

  lock_guard<mutex> l2(receiver_registration_lock_);
  // At this point receiver registrations must have happened.
  DCHECK(!receiver_map_.empty());
  ReceiverId id(receiver_node_id, receiver_instance_id);
  auto found = receiver_map_.find(id);
  DCHECK(found != receiver_map_.end());
  ReceiverRegistration& registration = found->second;
  DCHECK_NE(registration.receiver_, nullptr);

  LocalRowBatchChannel* channel = new LocalRowBatchChannel();
  registration.channels_.emplace_back(channel);
  channel->sender_cv_ = cv;
  channel->sender_id_ = sender_id;
  channel->receiver_ = registration.receiver_;
  // It is possible that the receiver has already unregistered. This is not an error,
  // the channel silently drop row batches in this case.
  if (registration.receiver_unregistered) channel->receiver_unregistered_ = true;
  *channel_out = channel;
  return Status::OK();
}

Status LocalRowBatchChannelManager::RegisterReceiver(
    const PlanNodeId& receiver_node_id, const TUniqueId& receiver_instance_id,
    KrpcDataStreamRecvr* receiver) {
  lock_guard<mutex> l(receiver_registration_lock_);
  ReceiverId id(receiver_node_id, receiver_instance_id);
  ReceiverRegistration& registration = receiver_map_[id];
  //LOG(INFO) << "receiver registered " << receiver_instance_id;
  DCHECK(!receiver_map_.empty());
  DCHECK_EQ(registration.receiver_, nullptr);
  registration.receiver_ = receiver;
  return Status::OK();
}

void LocalRowBatchChannelManager::UnregisterReceiver(const PlanNodeId& receiver_node_id,
    const TUniqueId& receiver_instance_id) {
  lock_guard<mutex> l(receiver_registration_lock_);
  ReceiverId id(receiver_node_id, receiver_instance_id);
  ReceiverRegistration& registration = receiver_map_[id];
  DCHECK_NE(registration.receiver_, nullptr);
  DCHECK(!registration.receiver_unregistered);
  registration.receiver_unregistered = true;
  for (unique_ptr<LocalRowBatchChannel>& channel: registration.channels_) {
    channel->UnregisterReceiver();
  }
}

void LocalRowBatchChannelManager::Cancel() {
  //LOG(INFO) << "LocalRowBatchChannelManager::Cancel() ";
  lock_guard<mutex> l1(sender_registration_lock_);
  lock_guard<mutex> l2(receiver_registration_lock_);
  for (auto& it: receiver_map_) {
    for (unique_ptr<LocalRowBatchChannel>& channel: it.second.channels_) {
      channel->Cancel();
    }
  }
  //LOG(INFO) << "LocalRowBatchChannelManager::Cancel() finished";
}

}
