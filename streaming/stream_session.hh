/*
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
 *
 * Modified by Cloudius Systems.
 * Copyright 2015 Cloudius Systems.
 */

#pragma once

#include "gms/i_endpoint_state_change_subscriber.hh"
#include "core/distributed.hh"
#include "message/messaging_service.hh"
#include "utils/UUID.hh"
#include "streaming/stream_session_state.hh"
#include "streaming/connection_handler.hh"
#include "streaming/stream_transfer_task.hh"
#include "streaming/stream_receive_task.hh"
#include "streaming/stream_request.hh"
#include "streaming/messages/incoming_file_message.hh"
#include "sstables/sstables.hh"
#include "query-request.hh"
#include "dht/i_partitioner.hh"
#include <map>
#include <vector>

namespace streaming {

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
class stream_session : public gms::i_endpoint_state_change_subscriber {
private:
    using messaging_verb = net::messaging_verb;
    using messaging_service = net::messaging_service;
    using shard_id = net::messaging_service::shard_id;
    using inet_address = gms::inet_address;
    using endpoint_state = gms::endpoint_state;
    using application_state = gms::application_state;
    using versioned_value = gms::versioned_value;
    using UUID = utils::UUID;
    using token = dht::token;
    net::messaging_service& ms() {
        return net::get_local_messaging_service();
    }
    class handler {
    public:
        future<> stop() {
            return make_ready_future<>();
        }
    };
    static distributed<handler> _handlers;
    void init_messaging_service_handler();
    future<> start();
public:
    struct ss_table_streaming_sections {
        sstables::sstable& sstable;
        std::map<int64_t, int64_t> sections;
        int64_t estimated_keys;
        int64_t repaired_at;
        ss_table_streaming_sections(sstables::sstable& sstable_, std::map<int64_t, int64_t> sections_,
                                    long estimated_keys_, long repaired_at_)
            : sstable(sstable_)
            , sections(std::move(sections_))
            , estimated_keys(estimated_keys_)
            , repaired_at(repaired_at_) {
        }
    };
public:
    /**
     * Streaming endpoint.
     *
     * Each {@code StreamSession} is identified by this InetAddress which is broadcast address of the node streaming.
     */
    inet_address peer;
    /** Actual connecting address. Can be the same as {@linkplain #peer}. */
    inet_address connecting;
    connection_handler conn_handler;
private:
    int _index;
    // should not be null when session is started
    //private StreamResultFuture streamResult;

    // stream requests to send to the peer
    std::vector<stream_request> _requests;
    // streaming tasks are created and managed per ColumnFamily ID
    std::map<UUID, stream_transfer_task> _transfers;
    // data receivers, filled after receiving prepare message
    std::map<UUID, stream_receive_task> _receivers;
    //private final StreamingMetrics metrics;
    /* can be null when session is created in remote */
    //private final StreamConnectionFactory factory;

    int _retries;
    bool _is_aborted =  false;
    bool _keep_ss_table_level;

    stream_session_state _state = stream_session_state::INITIALIZED;
    bool _complete_sent = false;
public:
    stream_session() : conn_handler(*this) { }
    /**
     * Create new streaming session with the peer.
     *
     * @param peer Address of streaming peer
     * @param connecting Actual connecting address
     * @param factory is used for establishing connection
     */
    stream_session(inet_address peer_, inet_address connecting_, int index_, bool keep_ss_table_level_)
        : peer(peer_)
        , connecting(connecting_)
        , conn_handler(*this)
        , _index(index_)
        , _keep_ss_table_level(keep_ss_table_level_) {
        //this.metrics = StreamingMetrics.get(connecting);
    }

    UUID plan_id() {
        // return streamResult == null ? null : streamResult.planId;
        // FIXME:
        return UUID();
    }

    int session_index() {
        return _index;
    }

#if 0

    public String description()
    {
        return streamResult == null ? null : streamResult.description;
    }
#endif
public:
    bool keep_ss_table_level() {
        return _keep_ss_table_level;
    }
#if 0
    /**
     * Bind this session to report to specific {@link StreamResultFuture} and
     * perform pre-streaming initialization.
     *
     * @param streamResult result to report to
     */
    public void init(StreamResultFuture streamResult)
    {
        this.streamResult = streamResult;
    }

    public void start()
    {
        if (requests.isEmpty() && transfers.isEmpty())
        {
            logger.info("[Stream #{}] Session does not have any tasks.", planId());
            close_session(stream_session_state::COMPLETE);
            return;
        }

        try
        {
            logger.info("[Stream #{}] Starting streaming to {}{}", planId(),
                                                                   peer,
                                                                   peer.equals(connecting) ? "" : " through " + connecting);
            handler.initiate();
            on_initialization_complete();
        }
        catch (Exception e)
        {
            JVMStabilityInspector.inspectThrowable(e);
            onError(e);
        }
    }

    public Socket createConnection() throws IOException
    {
        assert factory != null;
        return factory.createConnection(connecting);
    }
#endif

    /**
     * Request data fetch task to this session.
     *
     * @param keyspace Requesting keyspace
     * @param ranges Ranges to retrieve data
     * @param columnFamilies ColumnFamily names. Can be empty if requesting all CF under the keyspace.
     */
    void add_stream_request(sstring keyspace, std::vector<query::range<token>> ranges, std::vector<sstring> column_families, long repaired_at) {
        _requests.emplace_back(std::move(keyspace), std::move(ranges), std::move(column_families), repaired_at);
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
    void add_transfer_ranges(sstring keyspace, std::vector<query::range<token>> ranges, std::vector<sstring> column_families, bool flush_tables, long repaired_at) {
#if 0
        Collection<ColumnFamilyStore> stores = getColumnFamilyStores(keyspace, columnFamilies);
        if (flushTables)
            flushSSTables(stores);

        List<Range<Token>> normalizedRanges = Range.normalize(ranges);
        List<SSTableStreamingSections> sections = getSSTableSectionsForRanges(normalizedRanges, stores, repairedAt);
        try
        {
            addTransferFiles(sections);
        }
        finally
        {
            for (SSTableStreamingSections release : sections)
                release.sstable.releaseReference();
        }
#endif
    }

#if 0
    private Collection<ColumnFamilyStore> getColumnFamilyStores(String keyspace, Collection<String> columnFamilies)
    {
        Collection<ColumnFamilyStore> stores = new HashSet<>();
        // if columnfamilies are not specified, we add all cf under the keyspace
        if (columnFamilies.isEmpty())
        {
            stores.addAll(Keyspace.open(keyspace).getColumnFamilyStores());
        }
        else
        {
            for (String cf : columnFamilies)
                stores.add(Keyspace.open(keyspace).getColumnFamilyStore(cf));
        }
        return stores;
    }

    private List<SSTableStreamingSections> getSSTableSectionsForRanges(Collection<Range<Token>> ranges, Collection<ColumnFamilyStore> stores, long overriddenRepairedAt)
    {
        List<SSTableReader> sstables = new ArrayList<>();
        try
        {
            for (ColumnFamilyStore cfStore : stores)
            {
                List<AbstractBounds<RowPosition>> rowBoundsList = new ArrayList<>(ranges.size());
                for (Range<Token> range : ranges)
                    rowBoundsList.add(range.toRowBounds());
                ColumnFamilyStore.ViewFragment view = cfStore.selectAndReference(cfStore.viewFilter(rowBoundsList));
                sstables.addAll(view.sstables);
            }

            List<SSTableStreamingSections> sections = new ArrayList<>(sstables.size());
            for (SSTableReader sstable : sstables)
            {
                long repairedAt = overriddenRepairedAt;
                if (overriddenRepairedAt == ActiveRepairService.UNREPAIRED_SSTABLE)
                    repairedAt = sstable.getSSTableMetadata().repairedAt;
                sections.add(new SSTableStreamingSections(sstable,
                                                          sstable.getPositionsForRanges(ranges),
                                                          sstable.estimatedKeysForRanges(ranges),
                                                          repairedAt));
            }
            return sections;
        }
        catch (Throwable t)
        {
            SSTableReader.releaseReferences(sstables);
            throw t;
        }
    }
#endif

    void add_transfer_files(std::vector<ss_table_streaming_sections> sstable_details);

private:
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
    stream_session_state get_state() {
        return _state;
    }

    /**
     * Return if this session completed successfully.
     *
     * @return true if session completed successfully.
     */
    bool is_success() {
        return _state == stream_session_state::COMPLETE;
    }

#if 0
    public void messageReceived(StreamMessage message)
    {
        switch (message.type)
        {
            case PREPARE:
                PrepareMessage msg = (PrepareMessage) message;
                prepare(msg.requests, msg.summaries);
                break;

            case FILE:
                receive((IncomingFileMessage) message);
                break;

            case RECEIVED:
                ReceivedMessage received = (ReceivedMessage) message;
                received(received.cfId, received.sequenceNumber);
                break;

            case RETRY:
                RetryMessage retry = (RetryMessage) message;
                retry(retry.cfId, retry.sequenceNumber);
                break;

            case COMPLETE:
                complete();
                break;

            case SESSION_FAILED:
                sessionFailed();
                break;
        }
    }
#endif
    /**
     * Call back when connection initialization is complete to start the prepare phase.
     */
    void on_initialization_complete();

#if 0
    /**l
     * Call back for handling exception during streaming.
     *
     * @param e thrown exception
     */
    public void onError(Throwable e)
    {
        logger.error("[Stream #{}] Streaming error occurred", planId(), e);
        // send session failure message
        if (handler.isOutgoingConnected())
            handler.sendMessage(new SessionFailedMessage());
        // fail session
        close_session(stream_session_state::FAILED);
    }
#endif

    /**
     * Prepare this session for sending/receiving files.
     */
    void prepare(std::vector<stream_request> requests, std::vector<stream_summary> summaries);

#if 0
    /**
     * Call back after sending FileMessageHeader.
     *
     * @param header sent header
     */
    public void fileSent(FileMessageHeader header)
    {
        long headerSize = header.size();
        StreamingMetrics.totalOutgoingBytes.inc(headerSize);
        metrics.outgoingBytes.inc(headerSize);
        // schedule timeout for receiving ACK
        StreamTransferTask task = transfers.get(header.cfId);
        if (task != null)
        {
            task.scheduleTimeout(header.sequenceNumber, 12, TimeUnit.HOURS);
        }
    }
#endif

    /**
     * Call back after receiving FileMessageHeader.
     *
     * @param message received file
     */
    void receive(messages::incoming_file_message message);

    void progress(/* Descriptor desc */ progress_info::direction dir, long bytes, long total);

    void received(UUID cf_id, int sequence_number);

    /**
     * Call back on receiving {@code StreamMessage.Type.RETRY} message.
     *
     * @param cfId ColumnFamily ID
     * @param sequenceNumber Sequence number to indicate which file to stream again
     */
    void retry(UUID cf_id, int sequence_number);

    /**
     * Check if session is completed on receiving {@code StreamMessage.Type.COMPLETE} message.
     */
    void complete();

    /**
     * Call back on receiving {@code StreamMessage.Type.SESSION_FAILED} message.
     */
    void session_failed();

#if 0
    public void doRetry(FileMessageHeader header, Throwable e)
    {
        logger.warn("[Stream #{}] Retrying for following error", planId(), e);
        // retry
        retries++;
        if (retries > DatabaseDescriptor.getMaxStreamingRetries())
            onError(new IOException("Too many retries for " + header, e));
        else
            handler.sendMessage(new RetryMessage(header.cfId, header.sequenceNumber));
    }
#endif

    /**
     * @return Current snapshot of this session info.
     */
    session_info get_session_info();

    void task_completed(stream_receive_task& completed_task);

    void task_completed(stream_transfer_task& completed_task);

public:
    virtual void on_join(inet_address endpoint, endpoint_state ep_state) override {}
    virtual void before_change(inet_address endpoint, endpoint_state current_state, application_state new_state_key, versioned_value new_value) override {}
    virtual void on_change(inet_address endpoint, application_state state, versioned_value value) override {}
    virtual void on_alive(inet_address endpoint, endpoint_state state) override {}
    virtual void on_dead(inet_address endpoint, endpoint_state state) override {}
    virtual void on_remove(inet_address endpoint) override { close_session(stream_session_state::FAILED); }
    virtual void on_restart(inet_address endpoint, endpoint_state ep_state) override { close_session(stream_session_state::FAILED); }

private:
    bool maybe_completed();
#if 0

    /**
     * Flushes matching column families from the given keyspace, or all columnFamilies
     * if the cf list is empty.
     */
    private void flushSSTables(Iterable<ColumnFamilyStore> stores)
    {
        List<Future<?>> flushes = new ArrayList<>();
        for (ColumnFamilyStore cfs : stores)
            flushes.add(cfs.forceFlush());
        FBUtilities.waitOnFutures(flushes);
    }
#endif
    void prepare_receiving(stream_summary& summary);
    void start_streaming_files();
};

} // namespace streaming
