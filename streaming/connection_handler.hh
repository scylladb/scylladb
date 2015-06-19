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

#include "utils/UUID.hh"
#include "gms/inet_address.hh"
#include "streaming/stream_session.hh"
#include "streaming/session_info.hh"
#include "streaming/progress_info.hh"

namespace streaming {

/**
 * ConnectionHandler manages incoming/outgoing message exchange for the {@link StreamSession}.
 *
 * <p>
 * Internally, ConnectionHandler manages thread to receive incoming {@link StreamMessage} and thread to
 * send outgoing message. Messages are encoded/decoded on those thread and handed to
 * {@link StreamSession#messageReceived(org.apache.cassandra.streaming.messages.StreamMessage)}.
 */
class connection_handler {
public:
    connection_handler(stream_session session)
        : _session(std::move(session))
        , _incoming(_session)
        , _outgoing(_session) {
    }

#if 0
    /**
     * Set up incoming message handler and initiate streaming.
     *
     * This method is called once on initiator.
     *
     * @throws IOException
     */
    public void initiate() throws IOException
    {
        logger.debug("[Stream #{}] Sending stream init for incoming stream", session.planId());
        Socket incomingSocket = session.createConnection();
        incoming.start(incomingSocket, StreamMessage.CURRENT_VERSION);
        incoming.sendInitMessage(incomingSocket, true);

        logger.debug("[Stream #{}] Sending stream init for outgoing stream", session.planId());
        Socket outgoingSocket = session.createConnection();
        outgoing.start(outgoingSocket, StreamMessage.CURRENT_VERSION);
        outgoing.sendInitMessage(outgoingSocket, false);
    }

    /**
     * Set up outgoing message handler on receiving side.
     *
     * @param socket socket to use for {@link org.apache.cassandra.streaming.ConnectionHandler.OutgoingMessageHandler}.
     * @param version Streaming message version
     * @throws IOException
     */
    public void initiateOnReceivingSide(Socket socket, boolean isForOutgoing, int version) throws IOException
    {
        if (isForOutgoing)
            outgoing.start(socket, version);
        else
            incoming.start(socket, version);
    }

    public ListenableFuture<?> close()
    {
        logger.debug("[Stream #{}] Closing stream connection handler on {}", session.planId(), session.peer);

        ListenableFuture<?> inClosed = incoming == null ? Futures.immediateFuture(null) : incoming.close();
        ListenableFuture<?> outClosed = outgoing == null ? Futures.immediateFuture(null) : outgoing.close();

        return Futures.allAsList(inClosed, outClosed);
    }

    /**
     * Enqueue messages to be sent.
     *
     * @param messages messages to send
     */
    public void sendMessages(Collection<? extends StreamMessage> messages)
    {
        for (StreamMessage message : messages)
            sendMessage(message);
    }

    public void sendMessage(StreamMessage message)
    {
        if (outgoing.isClosed())
            throw new RuntimeException("Outgoing stream handler has been closed");

        outgoing.enqueue(message);
    }

    /**
     * @return true if outgoing connection is opened and ready to send messages
     */
    public boolean isOutgoingConnected()
    {
        return outgoing != null && !outgoing.isClosed();
    }
#endif

public:
    class message_handler {
    protected:
        stream_session& session;
        int protocol_version;
        message_handler(stream_session& session_) : session(session_) {
        }
#if 0
        protected Socket socket;

        private final AtomicReference<SettableFuture<?>> closeFuture = new AtomicReference<>();


        protected abstract String name();

        protected static DataOutputStreamAndChannel getWriteChannel(Socket socket) throws IOException
        {
            WritableByteChannel out = socket.getChannel();
            // socket channel is null when encrypted(SSL)
            if (out == null)
                out = Channels.newChannel(socket.getOutputStream());
            return new DataOutputStreamAndChannel(socket.getOutputStream(), out);
        }

        protected static ReadableByteChannel getReadChannel(Socket socket) throws IOException
        {
            ReadableByteChannel in = socket.getChannel();
            // socket channel is null when encrypted(SSL)
            return in == null
                 ? Channels.newChannel(socket.getInputStream())
                 : in;
        }

        public void sendInitMessage(Socket socket, boolean isForOutgoing) throws IOException
        {
            StreamInitMessage message = new StreamInitMessage(
                    FBUtilities.getBroadcastAddress(),
                    session.sessionIndex(),
                    session.planId(),
                    session.description(),
                    isForOutgoing,
                    session.keepSSTableLevel());
            ByteBuffer messageBuf = message.createMessage(false, protocolVersion);
            getWriteChannel(socket).write(messageBuf);
        }

        public void start(Socket socket, int protocolVersion)
        {
            this.socket = socket;
            this.protocolVersion = protocolVersion;

            new Thread(this, name() + "-" + session.peer).start();
        }

        public ListenableFuture<?> close()
        {
            // Assume it wasn't closed. Not a huge deal if we create a future on a race
            SettableFuture<?> future = SettableFuture.create();
            return closeFuture.compareAndSet(null, future)
                 ? future
                 : closeFuture.get();
        }

        public boolean isClosed()
        {
            return closeFuture.get() != null;
        }

        protected void signalCloseDone()
        {
            closeFuture.get().set(null);

            // We can now close the socket
            try
            {
                socket.close();
            }
            catch (IOException ignore) {}
        }
#endif
    };

    /**
     * Incoming streaming message handler
     */
    class incoming_message_handler : public message_handler {
    public:
        incoming_message_handler(stream_session& session) : message_handler(session) {
        }
        sstring name() {
            return "STREAM-IN";
        }
#if 0
        public void run()
        {
            try
            {
                ReadableByteChannel in = getReadChannel(socket);
                while (!isClosed())
                {
                    // receive message
                    StreamMessage message = StreamMessage.deserialize(in, protocolVersion, session);
                    // Might be null if there is an error during streaming (see FileMessage.deserialize). It's ok
                    // to ignore here since we'll have asked for a retry.
                    if (message != null)
                    {
                        logger.debug("[Stream #{}] Received {}", session.planId(), message);
                        session.messageReceived(message);
                    }
                }
            }
            catch (SocketException e)
            {
                // socket is closed
                close();
            }
            catch (Throwable t)
            {
                JVMStabilityInspector.inspectThrowable(t);
                session.onError(t);
            }
            finally
            {
                signalCloseDone();
            }
        }
#endif
    };

    /**
     * Outgoing file transfer thread
     */
    class outgoing_message_handler : public message_handler {
    public:
        outgoing_message_handler(stream_session& session) : message_handler(session) {
        }

        sstring name() {
            return "STREAM-OUT";
        }
#if 0

        /*
         * All out going messages are queued up into messageQueue.
         * The size will grow when received streaming request.
         *
         * Queue is also PriorityQueue so that prior messages can go out fast.
         */
        private final PriorityBlockingQueue<StreamMessage> messageQueue = new PriorityBlockingQueue<>(64, new Comparator<StreamMessage>()
        {
            public int compare(StreamMessage o1, StreamMessage o2)
            {
                return o2.getPriority() - o1.getPriority();
            }
        });

        public void enqueue(StreamMessage message)
        {
            messageQueue.put(message);
        }

        public void run()
        {
            try
            {
                DataOutputStreamAndChannel out = getWriteChannel(socket);

                StreamMessage next;
                while (!isClosed())
                {
                    if ((next = messageQueue.poll(1, TimeUnit.SECONDS)) != null)
                    {
                        logger.debug("[Stream #{}] Sending {}", session.planId(), next);
                        sendMessage(out, next);
                        if (next.type == StreamMessage.Type.SESSION_FAILED)
                            close();
                    }
                }

                // Sends the last messages on the queue
                while ((next = messageQueue.poll()) != null)
                    sendMessage(out, next);
            }
            catch (InterruptedException e)
            {
                throw new AssertionError(e);
            }
            catch (Throwable e)
            {
                session.onError(e);
            }
            finally
            {
                signalCloseDone();
            }
        }

        private void sendMessage(DataOutputStreamAndChannel out, StreamMessage message)
        {
            try
            {
                StreamMessage.serialize(message, out, protocolVersion, session);
            }
            catch (SocketException e)
            {
                session.onError(e);
                close();
            }
            catch (IOException e)
            {
                session.onError(e);
            }
        }
#endif
    };

private:
    stream_session _session;
    incoming_message_handler _incoming;
    outgoing_message_handler _outgoing;
};

} // namespace streaming
