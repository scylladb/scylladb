/*
 *
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include <seastar/core/distributed.hh>
#include "message/messaging_service_fwd.hh"
#include "streaming/stream_session_state.hh"
#include "streaming/stream_transfer_task.hh"
#include "streaming/stream_receive_task.hh"
#include "streaming/stream_request.hh"
#include "streaming/prepare_message.hh"
#include "streaming/stream_manager.hh"
#include "streaming/stream_reason.hh"
#include "streaming/session_info.hh"
#include "service/topology_guard.hh"
#include <map>
#include <vector>

namespace db {

class system_distributed_keyspace;

}

namespace service {
class migration_manager;
}

namespace db::view {

class view_update_generator;

}

namespace streaming {

class stream_result_future;

/**
 * Handles the streaming a one or more section of one of more sstables to and from a specific
 * remote node.
 *
 * Both this node and the remote one will create a similar symmetrical StreamSession. A streaming
 * session has the following life-cycle:
 *
 * 1. Connections Initialization
 *
 *   (a) A node (the initiator in the following) create a new StreamSession, initialize it (init())
 *       and then start it (start()). Start will create a {@link ConnectionHandler} that will create
 *       two connections to the remote node (the follower in the following) with whom to stream and send
 *       a StreamInit message. The first connection will be the incoming connection for the
 *       initiator, and the second connection will be the outgoing.
 *   (b) Upon reception of that StreamInit message, the follower creates its own StreamSession,
 *       initialize it if it still does not exist, and attach connecting socket to its ConnectionHandler
 *       according to StreamInit message's isForOutgoing flag.
 *   (d) When the both incoming and outgoing connections are established, StreamSession calls
 *       StreamSession#onInitializationComplete method to start the streaming prepare phase
 *       (StreamResultFuture.startStreaming()).
 *
 * 2. Streaming preparation phase
 *
 *   (a) This phase is started when the initiator onInitializationComplete() method is called. This method sends a
 *       PrepareMessage that includes what files/sections this node will stream to the follower
 *       (stored in a StreamTransferTask, each column family has it's own transfer task) and what
 *       the follower needs to stream back (StreamReceiveTask, same as above). If the initiator has
 *       nothing to receive from the follower, it goes directly to its Streaming phase. Otherwise,
 *       it waits for the follower PrepareMessage.
 *   (b) Upon reception of the PrepareMessage, the follower records which files/sections it will receive
 *       and send back its own PrepareMessage with a summary of the files/sections that will be sent to
 *       the initiator (prepare()). After having sent that message, the follower goes to its Streamning
 *       phase.
 *   (c) When the initiator receives the follower PrepareMessage, it records which files/sections it will
 *       receive and then goes to his own Streaming phase.
 *
 * 3. Streaming phase
 *
 *   (a) The streaming phase is started by each node (the sender in the follower, but note that each side
 *       of the StreamSession may be sender for some of the files) involved by calling startStreamingFiles().
 *       This will sequentially send a FileMessage for each file of each SteamTransferTask. Each FileMessage
 *       consists of a FileMessageHeader that indicates which file is coming and then start streaming the
 *       content for that file (StreamWriter in FileMessage.serialize()). When a file is fully sent, the
 *       fileSent() method is called for that file. If all the files for a StreamTransferTask are sent
 *       (StreamTransferTask.complete()), the task is marked complete (taskCompleted()).
 *   (b) On the receiving side, a SSTable will be written for the incoming file (StreamReader in
 *       FileMessage.deserialize()) and once the FileMessage is fully received, the file will be marked as
 *       complete (received()). When all files for the StreamReceiveTask have been received, the sstables
 *       are added to the CFS (and 2ndary index are built, StreamReceiveTask.complete()) and the task
 *       is marked complete (taskCompleted())
 *   (b) If during the streaming of a particular file an I/O error occurs on the receiving end of a stream
 *       (FileMessage.deserialize), the node will retry the file (up to DatabaseDescriptor.getMaxStreamingRetries())
 *       by sending a RetryMessage to the sender. On receiving a RetryMessage, the sender simply issue a new
 *       FileMessage for that file.
 *   (c) When all transfer and receive tasks for a session are complete, the move to the Completion phase
 *       (maybeCompleted()).
 *
 * 4. Completion phase
 *
 *   (a) When a node has finished all transfer and receive task, it enter the completion phase (maybeCompleted()).
 *       If it had already received a CompleteMessage from the other side (it is in the WAIT_COMPLETE state), that
 *       session is done is is closed (closeSession()). Otherwise, the node switch to the WAIT_COMPLETE state and
 *       send a CompleteMessage to the other side.
 */
class stream_session : public enable_shared_from_this<stream_session> {
private:
    using messaging_verb = netw::messaging_verb;
    using messaging_service = netw::messaging_service;
    using msg_addr = netw::msg_addr;
    using inet_address = gms::inet_address;
    using token = dht::token;
    using ring_position = dht::ring_position;

public:
    /**
     * Streaming endpoint.
     *
     * Each {@code StreamSession} is identified by this InetAddress which is broadcast address of the node streaming.
     */
    inet_address peer;
    unsigned dst_cpu_id = 0;
private:
    stream_manager& _mgr;
    // should not be null when session is started
    shared_ptr<stream_result_future> _stream_result;

    // stream requests to send to the peer
    std::vector<stream_request> _requests;
    // streaming tasks are created and managed per ColumnFamily ID
    std::map<table_id, stream_transfer_task> _transfers;
    // data receivers, filled after receiving prepare message
    std::map<table_id, stream_receive_task> _receivers;
    //private final StreamingMetrics metrics;
    /* can be null when session is created in remote */
    //private final StreamConnectionFactory factory;

    int64_t _bytes_sent = 0;
    int64_t _bytes_received = 0;

    int _retries;
    bool _is_aborted =  false;

    stream_session_state _state = stream_session_state::INITIALIZED;
    bool _complete_sent = false;
    bool _received_failed_complete_message = false;

    session_info _session_info;

    stream_reason _reason = stream_reason::unspecified;
    service::frozen_topology_guard _topo_guard;
public:
    stream_reason get_reason() const {
        return _reason;
    }
    void set_reason(stream_reason reason) {
        _reason = reason;
    }

    void set_topo_guard(service::frozen_topology_guard topo_guard) {
        _topo_guard = topo_guard;
    }

    service::frozen_topology_guard topo_guard() const {
        return _topo_guard;
    }

    void add_bytes_sent(int64_t bytes) {
        _bytes_sent += bytes;
    }

    void add_bytes_received(int64_t bytes) {
        _bytes_received += bytes;
    }

    int64_t get_bytes_sent() const {
        return _bytes_sent;
    }

    int64_t get_bytes_received() const {
        return _bytes_received;
    }
public:
    /**
     * Create new streaming session with the peer.
     *
     * @param peer Address of streaming peer
     * @param connecting Actual connecting address
     * @param factory is used for establishing connection
     */
    stream_session(stream_manager& mgr, inet_address peer_);
    ~stream_session();

    streaming::plan_id plan_id() const;

    sstring description() const;

public:
    /**
     * Bind this session to report to specific {@link StreamResultFuture} and
     * perform pre-streaming initialization.
     *
     * @param streamResult result to report to
     */
    void init(shared_ptr<stream_result_future> stream_result_);

    void start();

    bool is_initialized() const;

    /**
     * Request data fetch task to this session.
     *
     * @param keyspace Requesting keyspace
     * @param ranges Ranges to retrieve data
     * @param columnFamilies ColumnFamily names. Can be empty if requesting all CF under the keyspace.
     */
    void add_stream_request(sstring keyspace, dht::token_range_vector ranges, std::vector<sstring> column_families) {
        _requests.emplace_back(std::move(keyspace), std::move(ranges), std::move(column_families));
    }

    /**
     * Set up transfer for specific keyspace/ranges/CFs
     *
     * Used in repair - a streamed sstable in repair will be marked with the given repairedAt time
     *
     * @param keyspace Transfer keyspace
     * @param ranges Transfer ranges
     * @param columnFamilies Transfer ColumnFamilies
     * @param flushTables flush tables?
     * @param repairedAt the time the repair started.
     */
    void add_transfer_ranges(sstring keyspace, dht::token_range_vector ranges, std::vector<sstring> column_families);

    std::vector<replica::column_family*> get_column_family_stores(const sstring& keyspace, const std::vector<sstring>& column_families);

    void close_session(stream_session_state final_state);

public:
    /**
     * Set current state to {@code newState}.
     *
     * @param newState new state to set
     */
    void set_state(stream_session_state new_state) {
        _state = new_state;
    }

    /**
     * @return current state
     */
    stream_session_state get_state() const {
        return _state;
    }

    /**
     * Return if this session completed successfully.
     *
     * @return true if session completed successfully.
     */
    bool is_success() const {
        return _state == stream_session_state::COMPLETE;
    }

    future<> initiate();

    /**
     * Call back when connection initialization is complete to start the prepare phase.
     */
    future<> on_initialization_complete();

    /**l
     * Call back for handling exception during streaming.
     *
     * @param e thrown exception
     */
    void on_error();

    void abort();

    void received_failed_complete_message();

    /**
     * Prepare this session for sending/receiving files.
     */
    future<prepare_message> prepare(std::vector<stream_request> requests, std::vector<stream_summary> summaries);

    void follower_start_sent();

    /**
     * Check if session is completed on receiving {@code StreamMessage.Type.COMPLETE} message.
     */
    void complete();

    /**
     * @return Current snapshot of this session info.
     */
    session_info make_session_info();

    session_info& get_session_info() {
        return _session_info;
    }

    const session_info& get_session_info() const {
        return _session_info;
    }

    stream_manager& manager() noexcept { return _mgr; }
    const stream_manager& manager() const noexcept { return _mgr; }

    future<> update_progress();

    void receive_task_completed(table_id cf_id);
    void transfer_task_completed(table_id cf_id);
    void transfer_task_completed_all();
private:
    void send_failed_complete_message();
    bool maybe_completed();
    void prepare_receiving(stream_summary& summary);
    void start_streaming_files();
    future<> receiving_failed(table_id cf_id);
};

} // namespace streaming
