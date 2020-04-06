package com.jeffdisher.laminar.network;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.LinkedList;

import com.jeffdisher.laminar.utils.Assert;


/**
 * Common abstraction over a logical network use-case.  While it is possible to have all network use-cases share the
 * same manager, we will keep them distinct for the time being.
 * The manager maintains all sockets and buffers associated with this purpose and performs all interactions in its own
 * thread.
 * All interactions with it are asynchronous and CALLBACKS ARE SENT IN THE MANAGER'S THREAD.  This means that they must
 * only hand-off to the coordination thread, outside.
 * Note on message framing:  All network messages, including the header, are limited to 64 KiB.  The header is 2-bytes
 * (the big-endian u16 of the size) so this means the usable payload can be between [0..65534].
 * 
 * Concerns regarding concurrent access to NIO resources:
 * 1)  Registering a channel with a selector seems to block on any other interaction with the selector, mostly notably
 *  a blocking select operation.  For the most part, these registrations are only done on the internal but outgoing
 *  connections are created/destroyed on an external caller.  For this reasons, we will use a hand-off so that the
 *  internal thread wakes from the select operation to do the registration on behalf of the caller.
 * 2)  Changing the interested operations of an existing registration _appears_ to be safe and not blocked by an ongoing
 *  select operation so we will still change those on the outside.  This may be unreliable and the documentation states
 *  that this is implementation-dependent so this may chance in the future.
 */
public class NetworkManager {
	// We will use 64 KiB buffers since we will impose a message size limit less than this.
	private static final int BUFFER_SIZE_BYTES = 64 * 1024;
	// We reserve 2 bytes in the message payload for the big-endian u16 size so the maximum payload is 65534 bytes.
	public static final int MESSAGE_PAYLOAD_MAXIMUM_BYTES = 64 * 1024 - 2;

	private final Selector _selector;
	private final ServerSocketChannel _acceptorSocket;
	private final SelectionKey _acceptorKey;
	// Note that the _connectedNodes contains all connected sockets, incoming or outgoing.
	private final LinkedList<SelectionKey> _connectedNodes;
	private final INetworkManagerBackgroundCallbacks _callbackTarget;

	// Hand-offs used for opening/closing out-going connections.
	private volatile SocketChannel _handoff_newConnection;
	private volatile NodeToken _handoff_newConnectionResponse;
	private volatile NodeToken _handoff_closeConnection;

	// We will mark this volatile since we are relying on the select, not the monitor.
	private volatile boolean _keepRunning;
	private Thread _background;

	public NetworkManager(ServerSocketChannel clusterSocket, INetworkManagerBackgroundCallbacks callbackTarget) throws IOException {
		// This can throw IOException which always feels odd in a constructor so maybe this should be a static method.
		// (could be passed in but this seems like an internal concern)
		_selector = Selector.open();
		// Configure the cluster server socket for use with the selector.
		clusterSocket.configureBlocking(false);
		// If this isn't a server socket, this is a static error.
		int serverSocketOps = clusterSocket.validOps();
		Assert.assertTrue(SelectionKey.OP_ACCEPT == serverSocketOps);
		_acceptorSocket = clusterSocket;
		// Note that we normally put ConnectionState in attachment, but there isn't one for the server socket.
		_acceptorKey = clusterSocket.register(_selector, serverSocketOps, null);
		// We put the connected nodes in a LinkedList since we want a dense list, we rarely change it, and often need to walk, in-order.
		_connectedNodes = new LinkedList<>();
		_callbackTarget = callbackTarget;
	}

	public void startAndWaitForReady() {
		_keepRunning = true;
		_background = new Thread() {
			@Override
			public void run() {
				try {
					_backgroundThreadMain();
				} catch (IOException e) {
					// TODO:  Remove this exception from the method signature and handle each case as either a valid state or a finer-grained problem.
					Assert.unimplemented(e.getLocalizedMessage());
				}
			}
		};
		_background.start();
	}

	public void stopAndWaitForTermination() {
		// We can use the wakeup() method, instead of needing a pipe to break the select and a monitor to notify.
		_keepRunning = false;
		_selector.wakeup();
		// Wait for shutdown.
		try {
			_background.join();
		} catch (InterruptedException e) {
			// We don't use interruption.
			Assert.unexpected(e);
		}
		// Since we registered the initial selector, we need to cancel it.
		_acceptorKey.cancel();
		// We also expect the selector's key set to now be empty (we need to force it to update this).
		try {
			_selector.selectNow();
		} catch (IOException e) {
			// We are only doing this for purposes of updating the keys for the assert so we will ignore this.
		}
		Assert.assertTrue(_selector.keys().isEmpty());
	}

	/**
	 * Attempts to write the given message payload to the outgoing buffer, failing if the buffer can't fit the payload.
	 * Note that, if this returns false, the caller should wait for nodeWriteReady.
	 * 
	 * @param target The node where the message should be sent.
	 * @param payload The message payload to send.
	 * @return True if the payload was added to the write buffer, false if it couldn't fit.
	 * @throws IllegalArgumentException If the payload is larger than MESSAGE_PAYLOAD_MAXIMUM_BYTES.
	 */
	public boolean trySendMessage(NetworkManager.NodeToken target, byte[] payload) throws IllegalArgumentException {
		// We consider calls into the public interface on the internal thread to be statically incorrect re-entrance
		// errors, so those are assertions.
		Assert.assertTrue(Thread.currentThread() != _background);
		if (payload.length > MESSAGE_PAYLOAD_MAXIMUM_BYTES) {
			throw new IllegalArgumentException("Buffer size greater than " + MESSAGE_PAYLOAD_MAXIMUM_BYTES);
		}
		
		boolean didSend = false;
		ConnectionState state = (ConnectionState) target.actualKey.attachment();
		synchronized (state) {
			// See if there is enough space in the buffer for this message.
			int spaceRequired = payload.length + Short.BYTES;
			// The check above made sure that this size can fit, but it may show up as negative on the Java side.
			short size = (short) payload.length;
			ByteBuffer buffer = state.toWrite;
			if (buffer.remaining() >= spaceRequired) {
				boolean willNeedWriteInterest = (0 == buffer.position());
				buffer.putShort(size);
				buffer.put(payload);
				
				if (willNeedWriteInterest) {
					// If the buffer _was_ empty, we know we now need to change the interested ops.
					// Note that it is safe to write this thread, even though the background is consuming it (according
					// to docs).
					// We are doing this under state lock to avoid racing problems where this write may finish before we
					// update it (we should never attempt to write from a buffer which is empty).
					target.actualKey.interestOps(target.actualKey.interestOps() | SelectionKey.OP_WRITE);
					_selector.wakeup();
				}
				didSend = true;
			}
		}
		return didSend;
	}

	/**
	 * Reads a message payload from the incoming buffer for the given sender.
	 * If there is no complete message, null is returned.
	 * 
	 * @param sender The node from which the message was sent.
	 * @return The message payload or null if a complete message wasn't available.
	 */
	public byte[] readWaitingMessage(NetworkManager.NodeToken sender) {
		// We consider calls into the public interface on the internal thread to be statically incorrect re-entrance
		// errors, so those are assertions.
		Assert.assertTrue(Thread.currentThread() != _background);
		
		byte[] message = null;
		ConnectionState state = (ConnectionState) sender.actualKey.attachment();
		synchronized (state) {
			// Read the size.
			ByteBuffer buffer = state.toRead;
			if (buffer.position() >= Short.BYTES) {
				boolean willNeedReadInterest = (0 == buffer.remaining());
				buffer.flip();
				int size = Short.toUnsignedInt(buffer.getShort());
				if (buffer.remaining() >= size) {
					// We have enough data so read it and compact the buffer.
					message = new byte[size];
					buffer.get(message);
					buffer.compact();
					
					if (willNeedReadInterest) {
						// If the buffer was full, we now need to re-add the reading interest.
						// Note that it is safe to write this thread, even though the background is consuming it
						// (according to docs).
						// We are doing this under state lock to avoid racing problems where this buffer may fill before
						// we update it (we should never attempt to read into a buffer which is full).
						sender.actualKey.interestOps(sender.actualKey.interestOps() | SelectionKey.OP_READ);
						_selector.wakeup();
					}
				} else {
					// We can't do the read so rewind the position and "unflip".
					buffer.position(buffer.limit());
					buffer.limit(buffer.capacity());
				}
			}
		}
		return message;
	}

	public NodeToken createOutgoingConnection(InetSocketAddress address) throws IOException {
		// We consider calls into the public interface on the internal thread to be statically incorrect re-entrance
		// errors, so those are assertions.
		Assert.assertTrue(Thread.currentThread() != _background);
		
		// Create the socket as non-blocking and initiate the connection.
		// This is then added to our _persistentOutboundNodes set, since we want to re-establish these if they drop.
		SocketChannel outbound = SocketChannel.open();
		outbound.configureBlocking(false);
		boolean isImmediatelyConnected = outbound.connect(address);
		// TODO: Remove this once we can test if immediate connections happen and what the implications are, if it does.
		if (isImmediatelyConnected) {
			System.out.println("IMMEDIATE CONNECT");
		}
		// Note:  This part seems gratuitously complex but we want the _internal_ thread to register the connection and
		// interact with _connectedNodes, ONLY.
		// This allows enhanced safety around _connectedNodes but mostly it is to avoid an issue where registering with
		// the selector will block if an active select is in-process.
		// Also, since creating/destroying connections is a rare an expensive operation, this should be safe.
		NodeToken token = null;
		synchronized (this) {
			// Make sure we aren't queued behind someone else.
			while (null != _handoff_newConnection) {
				try {
					this.wait();
				} catch (InterruptedException e) {
					// We don't use interruption.
					Assert.unexpected(e);
				}
			}
			_handoff_newConnection = outbound;
			// Notify the internal thread (it isn't blocked in a monitor, but in the select).
			_selector.wakeup();
			while (null == _handoff_newConnectionResponse) {
				try {
					this.wait();
				} catch (InterruptedException e) {
					// We don't use interruption.
					Assert.unexpected(e);
				}
			}
			token = _handoff_newConnectionResponse;
			_handoff_newConnectionResponse = null;
		}
		return token;
	}

	public void closeOutgoingConnection(NodeToken token) throws IOException {
		SocketChannel channel = ((ConnectionState)(token.actualKey).attachment()).channel;
		// The _internal_ thread owns outgoing connection open/close so hand this off to them.
		synchronized (this) {
			// Make sure we aren't queued behind someone else.
			while (null != _handoff_closeConnection) {
				try {
					this.wait();
				} catch (InterruptedException e) {
					// We don't use interruption.
					Assert.unexpected(e);
				}
			}
			_handoff_closeConnection = token;
			// Notify the internal thread (it isn't blocked in a monitor, but in the select).
			_selector.wakeup();
			while (null != _handoff_closeConnection) {
				try {
					this.wait();
				} catch (InterruptedException e) {
					// We don't use interruption.
					Assert.unexpected(e);
				}
			}
		}
		// By this point, we can proceed to close the channel (symmetry with the opening of the channel).
		channel.close();
	}

	private void _backgroundThreadMain() throws IOException {
		while (_keepRunning) {
			int selectedKeyCount = _selector.select();
			// We are often just woken up to update the set of interested operations so this may be empty.
			if (selectedKeyCount > 0) {
				processSelectedKeys();
			}
			// Note that the keys' interested operations are updated by processing why they were selected or when an
			// external caller changes the state so we don't need to go through them, again.
			
			// Also, we may have an outgoing connection pending so check that.
			_backgroundCheckHandoff();
		}
		// We are shutting down so close all clients.
		for (SelectionKey key : _connectedNodes) {
			ConnectionState state = (ConnectionState)key.attachment();
			state.channel.close();
			key.cancel();
		}
	}

	private synchronized void _backgroundCheckHandoff() throws ClosedChannelException {
		if (null != _handoff_newConnection) {
			// Register this connection, pass back the token, and notify them.
			ConnectionState newState = new ConnectionState(_handoff_newConnection);
			SelectionKey newKey = _handoff_newConnection.register(_selector, SelectionKey.OP_CONNECT, newState);
			NodeToken token = new NodeToken(newKey);
			newState.token = token;
			_connectedNodes.add(newKey);
			
			_handoff_newConnection = null;
			// The last caller would have consumed the response before the new caller could have passed us work.
			Assert.assertTrue(null == _handoff_newConnectionResponse);
			_handoff_newConnectionResponse = token;
			
			this.notifyAll();
		}
		
		// Check the disconnect, as well.
		if (null != _handoff_closeConnection) {
			SelectionKey key = _handoff_closeConnection.actualKey;
			_connectedNodes.remove(key);
			key.cancel();
			// (note that we only cancel the registration - closing the socket is done in the calling thread).
			
			_handoff_closeConnection = null;
			this.notifyAll();
		}
	}

	private void processSelectedKeys() throws IOException, ClosedChannelException {
		Iterator<SelectionKey> selectedKeys = _selector.selectedKeys().iterator();
		while (selectedKeys.hasNext()) {
			SelectionKey key = selectedKeys.next();
			if (!key.isValid()) {
				// TODO:  Determine if this is how read/write failures on a closed connection are detected or if this is just us seeing the echo of something _we_ disconnected.
				System.err.println("INVALID KEY: " + key);
			}
			else if (key == _acceptorKey) {
				// This must be a new node connecting.
				Assert.assertTrue(SelectionKey.OP_ACCEPT == key.readyOps());
				SocketChannel newNode = _acceptorSocket.accept();
				
				// Configure this new node for our selection set - by default, it starts only waiting for read.
				newNode.configureBlocking(false);
				ConnectionState newState = new ConnectionState(newNode);
				SelectionKey newKey = newNode.register(_selector, SelectionKey.OP_READ, newState);
				NodeToken token = new NodeToken(newKey);
				newState.token = token;
				_connectedNodes.add(newKey);
				// Notify the callbacks.
				_callbackTarget.nodeDidConnect(token);
			} else {
				// This is normal data movement so get the state out of the attachment.
				ConnectionState state = (ConnectionState)key.attachment();
				// We can't fail to find this since we put it in the collection.
				Assert.assertTrue(null != state);
				
				// See what operation we wanted to perform.
				if (key.isConnectable()) {
					// Finish the connection, send the callback that an outbound connection was established, and switch
					// its interested ops (normally just READ but WRITE is possible if data has already been written to
					// the outgoing buffer.
					// NOTE: Outbound connections are NOT added to _connectedNodes.
					boolean isConnected = state.channel.finishConnect();
					if (!isConnected) {
						Assert.unimplemented("Does connection failure manifest here?");
					}
					int interestedOps = SelectionKey.OP_READ;
					synchronized (state) {
						if (state.toWrite.position() > 0) {
							interestedOps |= SelectionKey.OP_WRITE;
						}
					}
					key.interestOps(interestedOps);
					_callbackTarget.outboundNodeConnected(state.token);
				}
				if (key.isReadable()) {
					// Read into our buffer.
					int newMessagesAvailable = 0;
					synchronized (state) {
						int originalPosition = state.toRead.position();
						state.channel.read(state.toRead);
						
						if (0 == state.toRead.remaining()) {
							// If this buffer is now full, stop reading.
							key.interestOps(key.interestOps() & ~SelectionKey.OP_READ);
						}
						
						// Determine if this read operation added a new completed message to the buffer.
						ByteBuffer readOnly = state.toRead.asReadOnlyBuffer();
						readOnly.flip();
						boolean keepReading = true;
						while (keepReading && ((readOnly.position() + Short.BYTES) <= readOnly.limit())) {
							int size = Short.toUnsignedInt(readOnly.getShort());
							if ((readOnly.position() + size) <= readOnly.limit()) {
								readOnly.position(readOnly.position() + size);
								// If we are now pointing after the original position, this must be a new message.
								if (readOnly.position() > originalPosition) {
									newMessagesAvailable += 1;
								}
							} else {
								keepReading = false;
							}
						}
					}
					// Notify the callbacks of each new message.
					for (int i = 0; i < newMessagesAvailable; ++i) {
						_callbackTarget.nodeReadReady(state.token);
					}
				}
				if (key.isWritable()) {
					// Write from our buffer.
					boolean isBufferEmpty = false;
					synchronized (state) {
						state.toWrite.flip();
						state.channel.write(state.toWrite);
						state.toWrite.compact();
						
						isBufferEmpty = 0 == state.toWrite.position();
						if (isBufferEmpty) {
							// If this buffer is now empty, stop writing.
							key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);
						}
					}
					if (isBufferEmpty) {
						// We need to notify the callbacks that the buffer has fully drained.
						_callbackTarget.nodeWriteReady(state.token);
					}
				}
			}
			selectedKeys.remove();
		}
	}


	/**
	 * The internal representation of the buffer state around a connection.
	 * Note that all access to the buffers in this state MUST be done under the state's monitor.
	 */
	private static class ConnectionState {
		// Note that a channel is available directly from the SelectionKey but it is the wrong type so this is here to
		// avoid a down-cast (might switch to that later on but this keeps it clear).
		public final SocketChannel channel;
		public final ByteBuffer toRead = ByteBuffer.allocate(BUFFER_SIZE_BYTES);
		public final ByteBuffer toWrite = ByteBuffer.allocate(BUFFER_SIZE_BYTES);
		// Token is written after the key is created.
		public NodeToken token;
		
		public ConnectionState(SocketChannel channel) {
			this.channel = channel;
		}
	}


	/**
	 * This may seem like over-kill but this opaque wrapper of the SelectionKey is what we will use to represent a
	 * connection, outside of this class.
	 */
	public static class NodeToken {
		private final SelectionKey actualKey;
		private NodeToken(SelectionKey actualKey) {
			this.actualKey = actualKey;
		}
	}
}
