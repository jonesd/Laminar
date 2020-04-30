package com.jeffdisher.laminar.network;

import java.io.IOException;
import java.nio.channels.ServerSocketChannel;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;

import com.jeffdisher.laminar.components.INetworkManagerBackgroundCallbacks;
import com.jeffdisher.laminar.components.NetworkManager;
import com.jeffdisher.laminar.state.ClientState;
import com.jeffdisher.laminar.state.ListenerState;
import com.jeffdisher.laminar.state.ReconnectingClientState;
import com.jeffdisher.laminar.state.StateSnapshot;
import com.jeffdisher.laminar.types.ClientMessage;
import com.jeffdisher.laminar.types.ClientMessagePayload_Handshake;
import com.jeffdisher.laminar.types.ClientMessagePayload_Reconnect;
import com.jeffdisher.laminar.types.ClientResponse;
import com.jeffdisher.laminar.types.ClusterConfig;
import com.jeffdisher.laminar.types.ConfigEntry;
import com.jeffdisher.laminar.types.EventRecord;
import com.jeffdisher.laminar.types.MutationRecord;
import com.jeffdisher.laminar.utils.Assert;


/**
 * Top-level abstraction of a collection of network connections related to interactions with clients.
 * The manager maintains all sockets and buffers associated with this purpose and performs all interactions in its own
 * thread.
 * All interactions with it are asynchronous and CALLBACKS ARE SENT IN THE MANAGER'S THREAD.  This means that they must
 * only hand-off to the coordination thread, outside.
 */
public class ClientManager implements INetworkManagerBackgroundCallbacks {
	private final Thread _mainThread;
	private final NetworkManager _networkManager;
	private final IClientManagerCallbacks _callbacks;

	// The current leader of the cluster (null if we are the leader).  Clients will be redirected to it.
	private ConfigEntry _clusterLeader;
	// Note that we track clients in 1 of 3 different states:  new, normal, listener.
	// -new clients just connected and haven't yet sent a message so we don't know what they are doing.
	// -normal clients are the kind which send mutative operations and wait for acks
	// -listener clients are only listen to a stream of EventRecords
	// All clients start in "new" and move to "normal" or "listener" after their first message.
	private final Set<NetworkManager.NodeToken> _newClients;
	private final Map<NetworkManager.NodeToken, ClientState> _normalClientsByToken;
	private final Map<UUID, ClientState> _normalClientsById;
	private final Map<NetworkManager.NodeToken, ListenerState> _listenerClients;
	private final Map<Long, ClientCommitTuple> _pendingMessageCommits;
	// This is a map of local offsets to the list of listener clients waiting for them.
	// A key is only in this map if a load request for it is outstanding or it is an offset which hasn't yet been sent from a client.
	// The map uses the ClientNode since these may have disconnected.
	// Note that no listener should ever appear in _writableClients or _readableClients as these messages are sent immediately (since they would be unbounded buffers, otherwise).
	// This also means that there is currently no asynchronous load/send on the listener path as it is fully lock-step between network and disk.  This will be changed later.
	// (in the future, this will change to handle multiple topics).
	private final Map<Long, List<NetworkManager.NodeToken>> _listenersWaitingOnLocalOffset;
	private final Map<Long, List<ReconnectingClientState>> _reconnectingClientsByGlobalOffset;

	public ClientManager(ServerSocketChannel serverSocket, IClientManagerCallbacks callbacks) throws IOException {
		_mainThread = Thread.currentThread();
		// This is really just a high-level wrapper over the common NetworkManager so create that here.
		_networkManager = NetworkManager.bidirectional(serverSocket, this);
		_callbacks = callbacks;
		
		_newClients = new HashSet<>();
		_normalClientsByToken = new HashMap<>();
		_normalClientsById = new HashMap<>();
		_listenerClients = new HashMap<>();
		_pendingMessageCommits = new HashMap<>();
		_listenersWaitingOnLocalOffset = new HashMap<>();
		_reconnectingClientsByGlobalOffset = new HashMap<>();
	}

	/**
	 * Starts the underlying network connection, allowing clients to connect to it.
	 */
	public void startAndWaitForReady() {
		_networkManager.startAndWaitForReady("ClientManager");
	}

	/**
	 * Stops the underlying network and blocking until its thread has terminated.
	 */
	public void stopAndWaitForTermination() {
		_networkManager.stopAndWaitForTermination();
	}

	/**
	 * Reads and deserializes a message waiting from the given client.
	 * Note that this asserts that there was a message waiting.
	 * 
	 * @param client The client from which to read the message.
	 * @return The message.
	 */
	public ClientMessage receive(NetworkManager.NodeToken client) {
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		byte[] serialized = _networkManager.readWaitingMessage(client);
		// We only read when data is available.
		Assert.assertTrue(null != serialized);
		return ClientMessage.deserialize(serialized);
	}

	/**
	 * This is currently just used to implement the POISON method, for testing.
	 */
	public void mainDisconnectAllClientsAndListeners() {
		// Called on main thread.
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		for (NetworkManager.NodeToken node : _newClients) {
			_networkManager.closeConnection(node);
		}
		_newClients.clear();
		for (NetworkManager.NodeToken node : _normalClientsByToken.keySet()) {
			_networkManager.closeConnection(node);
		}
		_normalClientsByToken.clear();
		_normalClientsById.clear();
		for (NetworkManager.NodeToken node : _listenerClients.keySet()) {
			_networkManager.closeConnection(node);
		}
		_listenerClients.clear();
	}

	public void mainBroadcastConfigUpdate(StateSnapshot snapshot, ClusterConfig newConfig) {
		// Called on main thread.
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		// For the clients, we just enqueue this on all connected ones, directly.
		for (UUID clientId : _normalClientsById.keySet()) {
			ClientResponse update = ClientResponse.updateConfig(snapshot.lastCommittedMutationOffset, newConfig);
			_mainEnqueueMessageToClient(clientId, update);
		}
		// For listeners, we either need to send this directly (if they are already waiting for the next event)
		// or set their high-priority slot to preempt the next enqueue operation (since those ones are currently
		// waiting on an in-progress disk fetch).
		// We use the high-priority slot because it doesn't have a message queue and they don't need every update, just the most recent.
		EventRecord updateConfigPseudoRecord = EventRecord.synthesizeRecordForConfig(newConfig);
		// Set this in all of them and remove the ones we are going to eagerly handle.
		for (NetworkManager.NodeToken node : _listenerClients.keySet()) {
			ListenerState listenerState = _listenerClients.get(node);
			listenerState.highPriorityMessage = updateConfigPseudoRecord;
		}
		List<NetworkManager.NodeToken> waitingForNewEvent = _listenersWaitingOnLocalOffset.remove(snapshot.lastCommittedEventOffset);
		if (null != waitingForNewEvent) {
			for (NetworkManager.NodeToken node : waitingForNewEvent) {
				ListenerState listenerState = _listenerClients.get(node);
				// Make sure they are still connected.
				if (null != listenerState) {
					// Send this and clear it.
					_sendEventToListener(node, updateConfigPseudoRecord);
					Assert.assertTrue(updateConfigPseudoRecord == listenerState.highPriorityMessage);
					listenerState.highPriorityMessage = null;
				}
			}
		}
	}

	/**
	 * Called when the mutation commit offset changes.
	 * Any commit messages waiting on globalOffsetOfCommit must now be sent.
	 * 
	 * @param globalOffsetOfCommit The global mutation offset which is now committed.
	 */
	public void mainProcessingPendingMessageCommits(long globalOffsetOfCommit) {
		// Called on main thread.
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		// Look up the tuple so we know which clients and listeners should be told about the commit.
		ClientCommitTuple tuple = _pendingMessageCommits.remove(globalOffsetOfCommit);
		// This was requested for the specific tuple so it can't be missing.
		Assert.assertTrue(null != tuple);
		
		if (_normalClientsById.containsKey(tuple.clientId) && (null != _normalClientsById.get(tuple.clientId).noncesCommittedDuringReconnect)) {
			// Note that we can't allow already in-flight messages from before the reconnect to go to the client before the reconnect is done so enqueue nonces to commit here and we will synthesize them, later.
			_normalClientsById.get(tuple.clientId).noncesCommittedDuringReconnect.add(tuple.clientNonce);
		} else {
			// Create the commit from the information in the tuple and send it to the client.
			ClientResponse commit = ClientResponse.committed(tuple.clientNonce, globalOffsetOfCommit);
			_mainEnqueueMessageToClient(tuple.clientId, commit);
		}
	}

	public void mainReplayMutationForReconnects(StateSnapshot snapshot, MutationRecord record, boolean isCommitted) {
		// Called on main thread.
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		// See which syncing clients requested this (we will remove and rebuild the list since it is usually 1 element).
		List<ReconnectingClientState> reconnecting = _reconnectingClientsByGlobalOffset.remove(record.globalOffset);
		// This mutation may have been loaded for another syncing node, not a client, so check if we requested this.
		if (null != reconnecting) {
			List<ReconnectingClientState> moveToNext = new LinkedList<>();
			long nextMutationOffset = record.globalOffset + 1;
			for (ReconnectingClientState state : reconnecting) {
				// Make sure that the UUID matches.
				// (in the future, we might want this to aggressively check if the client is still connected to short-circuit multiple-reconnects from the same client)
				if (record.clientId.equals(state.clientId)) {
					// We need to synthesize a RECEIVED message and potentially a COMMITTED message for this mutation so the client knows not to re-send it and what they should wait for.
					// We want to make sure that we provide a lastCommitGlobalOffset which allows the client to make progress in case of a reconnect _during_ an in-progress reconnect.
					// If we kept sending their original lastCheckedGlobalOffset, then the second reconnect would have already remove in-flight messages they weill be told about.
					// If we always sent the offset of the specific record, then we would tell it we have committed messages which may not have committed.
					// Thus, we use mostRecentlySentServerCommitOffset and update it for every COMMITTED message we send.
					
					// Note that we won't send the committed if we think that we already have one pending for this reconnect (it already committed while the reconnect was in progress).
					boolean willSendCommitted = (isCommitted && (record.globalOffset <= state.finalCommitToReturnInReconnect));
					long lastCommitGlobalOffset = willSendCommitted
							? record.globalOffset
							: state.mostRecentlySentServerCommitOffset;
					_mainEnqueueMessageToClient(state.clientId, ClientResponse.received(record.clientNonce, lastCommitGlobalOffset));
					if (willSendCommitted) {
						_mainEnqueueMessageToClient(state.clientId, ClientResponse.committed(record.clientNonce, lastCommitGlobalOffset));
						state.mostRecentlySentServerCommitOffset = lastCommitGlobalOffset;
					}
					// Make sure to bump ahead the expected nonce, if this is later.
					if (record.clientNonce >= state.earliestNextNonce) {
						state.earliestNextNonce = record.clientNonce + 1;
					}
				}
				// Check if there is still more to see for this record (we might have run off the end of the latest commit when we started).
				if (nextMutationOffset <= state.finalGlobalOffsetToCheck) {
					moveToNext.add(state);
				} else {
					// Reconnect has concluded so make this client normal.
					_mainConcludeReconnectPhase(_normalClientsById.get(state.clientId), state.earliestNextNonce, snapshot.lastCommittedMutationOffset, snapshot.currentConfig);
				}
			}
			// Only move this list to the next offset if we still have more and there were any.
			if (!moveToNext.isEmpty()) {
				List<ReconnectingClientState> nextList = _reconnectingClientsByGlobalOffset.get(nextMutationOffset);
				if (null != nextList) {
					nextList.addAll(moveToNext);
				} else {
					_reconnectingClientsByGlobalOffset.put(nextMutationOffset, moveToNext);
					// We added somethod new for this offset so fetch it.
					_callbacks.mainRequestMutationFetch(nextMutationOffset);
				}
			}
		}
	}

	public void mainSendRecordToListeners(EventRecord record) {
		// Called on main thread.
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		_mainSendRecordToListeners(record);
	}

	/**
	 * This helper exists purely for testing purposes.  It will assert if there are more than 1 connected clients in new
	 * state.
	 * 
	 * @return The only client in "new" state or null, if there aren't any.
	 */
	public NetworkManager.NodeToken testingGetOneClientNode() {
		NetworkManager.NodeToken toReturn = null;
		if (!_newClients.isEmpty()) {
			Assert.assertTrue(1 == _newClients.size());
			toReturn = _newClients.iterator().next();
		}
		return toReturn;
	}

	/**
	 * Called when the node has entered a FOLLOWER state, in the cluster.
	 * This means sending a redirect to all current and future clients.
	 * We will leave listeners connected, though, as they will be able to listen to a FOLLOWER.
	 * 
	 * @param clusterLeader The new leader of the cluster.
	 * @param lastCommittedMutationOffset The offset of the most recent mutation the server committed.
	 */
	public void mainEnterFollowerState(ConfigEntry clusterLeader, long lastCommittedMutationOffset) {
		// Set the config entry for future connection redirects.
		_clusterLeader = clusterLeader;
		
		// Send this to all clients (not new clients since we don't yet know if they are normal clients or listeners).
		ClientResponse redirect = ClientResponse.redirect(_clusterLeader, lastCommittedMutationOffset);
		for (UUID client : _normalClientsById.keySet()) {
			_mainEnqueueMessageToClient(client, redirect);
		}
	}

	@Override
	public void nodeDidConnect(NetworkManager.NodeToken node) {
		// We handle this here but we ask to handle this on a main thread callback.
		_callbacks.ioEnqueueClientCommandForMainThread(new Consumer<StateSnapshot>() {
			@Override
			public void accept(StateSnapshot arg) {
				Assert.assertTrue(Thread.currentThread() == _mainThread);
				// A fresh connection is a new client.
				_newClients.add(node);
			}});
	}

	@Override
	public void nodeDidDisconnect(NetworkManager.NodeToken node, IOException cause) {
		// We handle this here but we ask to handle this on a main thread callback.
		_callbacks.ioEnqueueClientCommandForMainThread(new Consumer<StateSnapshot>() {
			@Override
			public void accept(StateSnapshot arg) {
				Assert.assertTrue(Thread.currentThread() == _mainThread);
				// A disconnect is a transition for all clients so try to remove from them all.
				// Note that this node may still be in an active list but since we always resolve it against this map, we will fail to resolve it.
				// We add a check to make sure that this is consistent.
				boolean removedNew = _newClients.remove(node);
				ClientState removedClient = _normalClientsByToken.remove(node);
				if (null != removedClient) {
					_normalClientsById.remove(removedClient.clientId);
				}
				boolean removedNormal = (null != removedClient);
				boolean removedListener = (null != _listenerClients.remove(node));
				boolean removeConsistent = false;
				if (removedNew) {
					System.out.println("Disconnect new client");
					removeConsistent = true;
				}
				if (removedNormal) {
					Assert.assertTrue(!removeConsistent);
					System.out.println("Disconnect normal client");
					removeConsistent = true;
				}
				if (removedListener) {
					Assert.assertTrue(!removeConsistent);
					System.out.println("Disconnect listener client");
					removeConsistent = true;
				}
				Assert.assertTrue(removeConsistent);
			}});
	}

	@Override
	public void nodeWriteReady(NetworkManager.NodeToken node) {
		// Called on IO thread.
		Assert.assertTrue(Thread.currentThread() != _mainThread);
		// We handle this here but we ask to handle this on a main thread callback.
		_callbacks.ioEnqueueClientCommandForMainThread(new Consumer<StateSnapshot>() {
			@Override
			public void accept(StateSnapshot arg) {
				Assert.assertTrue(Thread.currentThread() == _mainThread);
				// Set the writable flag or send a pending message.
				// Clients start in the ready state so this couldn't be a new client (since we never sent them a message).
				Assert.assertTrue(!_newClients.contains(node));
				ClientState normalState = _normalClientsByToken.get(node);
				ListenerState listenerState = _listenerClients.get(node);
				if (null != normalState) {
					Assert.assertTrue(null == listenerState);
					// Normal client.
					// This can't already be writable.
					Assert.assertTrue(!normalState.writable);
					// Check to see if there are any outgoing messages.  If so, just send the first.  Otherwise, set the writable flag.
					if (normalState.outgoingMessages.isEmpty()) {
						normalState.writable = true;
					} else {
						ClientResponse toSend = normalState.outgoingMessages.remove(0);
						_send(node, toSend);
					}
				} else if (null != listenerState) {
					// Listener.
					// The socket is now writable so first check if there is a high-priority message waiting.
					if (null != listenerState.highPriorityMessage) {
						// Send the high-priority message and we will proceed to sync when we get the next writable callback.
						_sendEventToListener(node, listenerState.highPriorityMessage);
						listenerState.highPriorityMessage = null;
					} else {
						// Normal syncing operation so either load or wait for the next event for this listener.
						long nextLocalEventToFetch = _mainSetupListenerForNextEvent(node, listenerState, arg.lastCommittedEventOffset);
						if (-1 != nextLocalEventToFetch) {
							_callbacks.mainRequestEventFetch(nextLocalEventToFetch);
						}
					}
				} else {
					// This appears to have disconnected before we processed it.
					System.out.println("NOTE: Processed write ready from disconnected client");
				}
			}});
	}

	@Override
	public void nodeReadReady(NetworkManager.NodeToken node) {
		// Called on an IO thread.
		Assert.assertTrue(Thread.currentThread() != _mainThread);
		_callbacks.ioEnqueueClientCommandForMainThread(new Consumer<StateSnapshot>() {
			@Override
			public void accept(StateSnapshot arg) {
				Assert.assertTrue(Thread.currentThread() == _mainThread);
				// Check what state the client is in.
				boolean isNew = _newClients.contains(node);
				ClientState normalState = _normalClientsByToken.get(node);
				ListenerState listenerState = _listenerClients.get(node);
				ClientMessage incoming = receive(node);
				
				if (isNew) {
					Assert.assertTrue(null == normalState);
					Assert.assertTrue(null == listenerState);
					
					// We will ignore the nonce on a new connection.
					// Note that the client maps are modified by this helper.
					long mutationOffsetToFetch = _mainTransitionNewConnectionState(node, incoming, arg.lastReceivedMutationOffset, arg.lastCommittedMutationOffset, arg.currentConfig, arg.lastCommittedEventOffset);
					if (-1 != mutationOffsetToFetch) {
						_callbacks.mainRequestMutationFetch(mutationOffsetToFetch);
					}
				} else if (null != normalState) {
					Assert.assertTrue(null == listenerState);
					
					// We can do the nonce check here, before we enter the state machine for the specific message type/contents.
					if (normalState.nextNonce == incoming.nonce) {
						normalState.nextNonce += 1;
						long globalMutationOffsetOfAcceptedMessage = _callbacks.mainHandleValidClientMessage(normalState.clientId, incoming);
						Assert.assertTrue(globalMutationOffsetOfAcceptedMessage > 0L);
						// We enqueue the ack, immediately.
						ClientResponse ack = ClientResponse.received(incoming.nonce, arg.lastCommittedMutationOffset);
						_mainEnqueueMessageToClient(normalState.clientId, ack);
						// Set up the client to be notified that the message committed once the MutationRecord is durable.
						_pendingMessageCommits.put(globalMutationOffsetOfAcceptedMessage, new ClientCommitTuple(normalState.clientId, incoming.nonce));
					} else {
						_mainEnqueueMessageToClient(normalState.clientId, ClientResponse.error(incoming.nonce, arg.lastCommittedMutationOffset));
					}
				} else if (null != listenerState) {
					// Once a listener is in the listener state, they should never send us another message.
					Assert.unimplemented("TODO: Disconnect listener on invalid state transition");
				} else {
					// This appears to have disconnected before we processed it.
					System.out.println("NOTE: Processed read ready from disconnected client");
				}
			}});
	}

	@Override
	public void outboundNodeConnected(NetworkManager.NodeToken node) {
		// This implementation has no outbound connections.
		Assert.unreachable("No outbound connections");
	}

	@Override
	public void outboundNodeDisconnected(NetworkManager.NodeToken node, IOException cause) {
		// This implementation has no outbound connections.
		Assert.unreachable("No outbound connections");
	}

	@Override
	public void outboundNodeConnectionFailed(NetworkManager.NodeToken token, IOException cause) {
		// This implementation has no outbound connections.
		Assert.unreachable("No outbound connections");
	}


	private long _mainTransitionNewConnectionState(NetworkManager.NodeToken client, ClientMessage incoming, long lastReceivedMutationOffset, long lastCommittedMutationOffset, ClusterConfig currentConfig, long lastCommittedEventOffset) {
		long mutationOffsetToFetch = -1;
		// Main thread helper.
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		switch (incoming.type) {
		case INVALID:
			Assert.unimplemented("Invalid message type - disconnect client");
			break;
		case HANDSHAKE: {
			// This is the first message a client sends us in order to make sure we know their UUID and the version is
			// correct and any other options are supported.
			UUID clientId = ((ClientMessagePayload_Handshake)incoming.payload).clientId;
			// Create the new state and change the connection state in the maps.
			ClientState state = _createAndInstallNewClient(client, clientId, 1L);
			
			if (null == _clusterLeader) {
				// The HANDSHAKE is special so we return CLIENT_READY instead of a RECEIVED and COMMIT (which is otherwise all we send).
				ClientResponse ready = ClientResponse.clientReady(state.nextNonce, lastCommittedMutationOffset, currentConfig);
				System.out.println("HANDSHAKE: " + state.clientId);
				_mainEnqueueMessageToClient(clientId, ready);
			} else {
				// Someone else is the leader so send them a redirect.
				// On new connections, the redirect shouldn't pretend to know what the leader has committed (could confuse client).
				ClientResponse redirect = ClientResponse.redirect(_clusterLeader, 0L);
				System.out.println("REDIRECT: " + state.clientId);
				_mainEnqueueMessageToClient(clientId, redirect);
			}
			break;
		}
		case RECONNECT: {
			// This is the first message an old client sends when it reconnects after a network failure or cluster fail-over.
			// This is like a HANDSHAKE but more complex in that we need to re-read our history to see if anything they
			// sent us before the disconnect actually did commit, even though we couldn't tell them it did (so they know
			// what to re-send).
			ClientMessagePayload_Reconnect reconnect = (ClientMessagePayload_Reconnect)incoming.payload;
			
			// Note that, even though we haven't sent them the CLIENT_READY message yet, we still add this to our
			// _normalClients map because the network interaction state machine is exactly the same.
			// (set a bogus nonce to fail if we receive anything before we finish and set it).
			ClientState state = _createAndInstallNewClient(client, reconnect.clientId, -1L);
			// This is a reconnecting client so set the buffer for old messages trying to commit back to this client.
			state.noncesCommittedDuringReconnect = new LinkedList<>();
			
			// We still use a specific ReconnectionState to track the progress of the sync.
			// We don't add them to our map of _normalClients until they finish syncing so we put them into our map of
			// _reconnectingClients and _reconnectingClientsByGlobalOffset.
			ReconnectingClientState reconnectState = new ReconnectingClientState(client, reconnect.clientId, incoming.nonce, reconnect.lastCommitGlobalOffset, lastReceivedMutationOffset, lastCommittedMutationOffset);
			boolean doesRequireFetch = false;
			boolean isPerformingReconnect = false;
			long offsetToFetch = reconnectState.lastCheckedGlobalOffset + 1;
			if (offsetToFetch <= reconnectState.finalGlobalOffsetToCheck) {
				List<ReconnectingClientState> reconnectingList = _reconnectingClientsByGlobalOffset.get(offsetToFetch);
				if (null == reconnectingList) {
					reconnectingList = new LinkedList<>();
					_reconnectingClientsByGlobalOffset.put(offsetToFetch, reconnectingList);
					// If we added an element to this map, request the mutation (otherwise, it is already coming).
					doesRequireFetch = true;
				}
				reconnectingList.add(reconnectState);
				isPerformingReconnect = true;
			}
			if (doesRequireFetch) {
				mutationOffsetToFetch = offsetToFetch;
			} else if (!isPerformingReconnect) {
				// This is a degenerate case where they didn't miss anything so just send them the client ready.
				System.out.println("Note:  RECONNECT degenerate case: " + state.clientId + " with nonce " + incoming.nonce);
				_mainConcludeReconnectPhase(state, reconnectState.earliestNextNonce, lastCommittedMutationOffset, currentConfig);
			}
			break;
		}
		case LISTEN: {
			// This is the first message a client sends when they want to register as a listener.
			// In this case, they won't send any other messages to us and just expect a constant stream of raw EventRecords to be sent to them.
			
			// Note that this message overloads the nonce as the last received local offset.
			long lastReceivedLocalOffset = incoming.nonce;
			if ((lastReceivedLocalOffset < 0L) || (lastReceivedLocalOffset > lastCommittedEventOffset)) {
				Assert.unimplemented("This listener is invalid so disconnect it");
			}
			
			// Create the new state and change the connection state in the maps.
			ListenerState state = new ListenerState(lastReceivedLocalOffset);
			boolean didRemove = _newClients.remove(client);
			Assert.assertTrue(didRemove);
			_listenerClients.put(client, state);
			
			// In this case, we will synthesize the current config as an EventRecord (we have a special type for config
			// changes), send them that message, and then we will wait for the socket to become writable, again, where
			// we will begin streaming EventRecords to them.
			EventRecord initialConfig = EventRecord.synthesizeRecordForConfig(currentConfig);
			// This is the only message we send on this socket which isn't specifically related to the
			// "fetch+send+repeat" cycle so we will send it directly, waiting for the writable callback from the socket
			// to start the initial fetch.
			// If we weren't sending a message here, we would start the cycle by calling _backgroundSetupListenerForNextEvent(), given that the socket starts writable.
			_sendEventToListener(client, initialConfig);
			break;
		}
		default:
			Assert.unimplemented("This is an invalid message for this client type and should be disconnected");
			break;
		}
		return mutationOffsetToFetch;
	}

	private void _mainEnqueueMessageToClient(UUID clientId, ClientResponse ack) {
		// Main thread helper.
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		// Look up the client to make sure they are still connected (messages to disconnected clients are just dropped).
		ClientState state = _normalClientsById.get(clientId);
		if (null != state) {
			// See if the client is writable.
			if (state.writable) {
				// Just write the message.
				_send(state.token, ack);
				state.writable = false;
			} else {
				// Writing not yet available so add this to the list.
				state.outgoingMessages.add(ack);
			}
		}
	}

	private long _mainSetupListenerForNextEvent(NetworkManager.NodeToken client, ListenerState state, long lastCommittedEventOffset) {
		// Main thread helper.
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		// See if there is a pending request for this offset.
		long nextLocalOffset = state.lastSentLocalOffset + 1;
		long nextLocalEventToFetch = -1;
		List<NetworkManager.NodeToken> waitingList = _listenersWaitingOnLocalOffset.get(nextLocalOffset);
		if (null == waitingList) {
			// Nobody is currently waiting so set up the record.
			waitingList = new LinkedList<>();
			_listenersWaitingOnLocalOffset.put(nextLocalOffset, waitingList);
			// Unless this is the next offset (meaning we are waiting for a client to send and commit it), then request the load.
			// Note that this, like all uses of "local" or "event-based" offsets, will eventually need to be per-topic.
			if (nextLocalOffset <= lastCommittedEventOffset) {
				// We will need this entry fetched.
				nextLocalEventToFetch = nextLocalOffset;
			}
		}
		waitingList.add(client);
		return nextLocalEventToFetch;
	}

	private void _mainSendRecordToListeners(EventRecord record) {
		// Main thread helper.
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		List<NetworkManager.NodeToken> waitingList = _listenersWaitingOnLocalOffset.remove(record.localOffset);
		if (null != waitingList) {
			for (NetworkManager.NodeToken node : waitingList) {
				ListenerState listenerState = _listenerClients.get(node);
				// (make sure this is still connected)
				if (null != listenerState) {
					_sendEventToListener(node, record);
					// Update the state to be the offset of this event.
					listenerState.lastSentLocalOffset = record.localOffset;
				}
			}
		}
	}

	private void _send(NetworkManager.NodeToken client, ClientResponse toSend) {
		byte[] serialized = toSend.serialize();
		boolean didSend = _networkManager.trySendMessage(client, serialized);
		// We only send when ready.
		Assert.assertTrue(didSend);
	}

	private void _sendEventToListener(NetworkManager.NodeToken client, EventRecord toSend) {
		byte[] serialized = toSend.serialize();
		boolean didSend = _networkManager.trySendMessage(client, serialized);
		// We only send when ready.
		Assert.assertTrue(didSend);
	}

	private ClientState _createAndInstallNewClient(NetworkManager.NodeToken client, UUID clientId, long nextNonce) {
		ClientState state = new ClientState(clientId, client, nextNonce);
		boolean didRemove = _newClients.remove(client);
		Assert.assertTrue(didRemove);
		// If we already have a client with this UUID, remove any old one (probably just a stale connection we haven't yet noticed is broken).
		if (_normalClientsById.containsKey(clientId)) {
			ClientState oldState = _normalClientsById.remove(clientId);
			_normalClientsByToken.remove(oldState.token);
			_networkManager.closeConnection(oldState.token);
		}
		_normalClientsByToken.put(client, state);
		_normalClientsById.put(clientId, state);
		return state;
	}

	private void _mainConcludeReconnectPhase(ClientState state, long earliestNextNonce, long lastCommittedMutationOffset, ClusterConfig currentConfig) {
		// Send them the client ready.
		ClientResponse ready = ClientResponse.clientReady(earliestNextNonce, lastCommittedMutationOffset, currentConfig);
		_mainEnqueueMessageToClient(state.clientId, ready);
		
		// Make sure we still have our sentinel nonce value and replace it.
		Assert.assertTrue(-1L == state.nextNonce);
		state.nextNonce = earliestNextNonce;
		
		// Synthesize commit messages for any nonces which accumulated during reconnect.
		Assert.assertTrue(null != state.noncesCommittedDuringReconnect);
		for (long nonceToCommit : state.noncesCommittedDuringReconnect) {
			ClientResponse synthesizedCommit = ClientResponse.committed(nonceToCommit, lastCommittedMutationOffset);
			_mainEnqueueMessageToClient(state.clientId, synthesizedCommit);
		}
		state.noncesCommittedDuringReconnect = null;
	}
}
