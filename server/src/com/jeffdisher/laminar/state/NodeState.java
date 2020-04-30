package com.jeffdisher.laminar.state;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;

import com.jeffdisher.laminar.console.ConsoleManager;
import com.jeffdisher.laminar.console.IConsoleManagerBackgroundCallbacks;
import com.jeffdisher.laminar.disk.DiskManager;
import com.jeffdisher.laminar.disk.IDiskManagerBackgroundCallbacks;
import com.jeffdisher.laminar.network.ClientManager;
import com.jeffdisher.laminar.network.ClusterManager;
import com.jeffdisher.laminar.network.IClientManagerCallbacks;
import com.jeffdisher.laminar.network.IClusterManagerCallbacks;
import com.jeffdisher.laminar.types.ClientMessage;
import com.jeffdisher.laminar.types.ClientMessagePayload_Temp;
import com.jeffdisher.laminar.types.ClientMessagePayload_UpdateConfig;
import com.jeffdisher.laminar.types.ClusterConfig;
import com.jeffdisher.laminar.types.ConfigEntry;
import com.jeffdisher.laminar.types.EventRecord;
import com.jeffdisher.laminar.types.EventRecordType;
import com.jeffdisher.laminar.types.MutationRecord;
import com.jeffdisher.laminar.types.MutationRecordType;
import com.jeffdisher.laminar.utils.Assert;
import com.jeffdisher.laminar.utils.UninterruptableQueue;


/**
 * Maintains the state of this specific node.
 * Primarily, this is where the main coordination thread sleeps until events are handed off to it.
 * Note that this "main thread" is actually the thread which started executing the program.  It is not started here.
 * Note that the thread which creates this instance is defined as "main" and MUST be the same thread which calls
 * runUntilShutdown() and MUST NOT call any background* methods (this is to verify re-entrance safety, etc).
 */
public class NodeState implements IClientManagerCallbacks, IClusterManagerCallbacks, IDiskManagerBackgroundCallbacks, IConsoleManagerBackgroundCallbacks {
	// We keep the main thread for asserting no re-entrance bugs or invalid interface uses.
	private final Thread _mainThread;

	private ClientManager _clientManager;
	private ClusterManager _clusterManager;
	private DiskManager _diskManager;
	private ConsoleManager _consoleManager;

	private RaftState _currentState;
	private ConfigEntry _clusterLeader;
	private boolean _leaderIsWritable;
	private long _ackToSendToLeader;
	private final ConfigEntry _self;
	// We keep an image of ourself as a downstream peer state to avoid special-cases in looking at clusters so we will need to update it with latest mutation offset as soon as we assign one.
	private final DownstreamPeerState _selfState;
	// The union of all config entries we are currently monitoring (normally just from the current config but could be all in joint consensus).
	private final Map<ConfigEntry, DownstreamPeerState> _unionOfDownstreamNodes;
	private SyncProgress _currentConfig;
	// This map is usually empty but contains any ClusterConfigs which haven't yet committed (meaning they are part of joint consensus).
	private final Map<Long, SyncProgress> _configsPendingCommit;
	// The next global mutation offset to assign to an incoming message.
	private long _nextGlobalMutationOffset;
	// Note that "local" event offsets will eventually need to be per-topic.
	private long _nextLocalEventOffset;
	// The offset of the mutation most recently committed to disk (used to keep both the clients and other nodes in sync).
	private long _lastCommittedMutationOffset;
	// Note that event offsets will eventually need to be per-topic.
	private long _lastCommittedEventOffset;

	// Tracking of in-flight mutations ready to be committed when the cluster agrees.
	// These must be committed in-order, so they are a queue with a base offset bias.
	// (note that the Events are synthesized from these mutations at the point of commit and _nextLocalEventOffset is updated then)
	// Note that we use a LinkedList since we want this to be addressable but also implement Queue.
	private LinkedList<InFlightTuple> _inFlightMutations;
	private long _inFlightMutationOffsetBias;

	// Information related to the state of the main execution thread.
	private boolean _keepRunning;
	private final UninterruptableQueue<StateSnapshot> _commandQueue;

	public NodeState(ClusterConfig initialConfig) {
		// We define the thread which instantiates us as "main".
		_mainThread = Thread.currentThread();
		// Note that we default to the LEADER state (typically forced into a FOLLOWER state when an existing LEADER attempts to append entries).
		_currentState = RaftState.LEADER;
		
		// We rely on the initial config just being "self".
		Assert.assertTrue(1 == initialConfig.entries.length);
		_self = initialConfig.entries[0];
		_selfState = new DownstreamPeerState();
		_selfState.isConnectionUp = true;
		_unionOfDownstreamNodes = new HashMap<>();
		_unionOfDownstreamNodes.put(_self, _selfState);
		_currentConfig = new SyncProgress(initialConfig, Collections.singleton(_selfState));
		_configsPendingCommit = new HashMap<>();
		
		// Global offsets are 1-indexed so the first one is 1L.
		_nextGlobalMutationOffset = 1L;
		_nextLocalEventOffset = 1L;
		
		// The first mutation has offset 1L so we use that as the initial bias.
		_inFlightMutations = new LinkedList<>();
		_inFlightMutationOffsetBias = 1L;
		
		_commandQueue = new UninterruptableQueue<>();
	}

	public void runUntilShutdown() {
		// This MUST be called on the main thread.
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		// Not fully configuring the instance is a programming error.
		Assert.assertTrue(null != _clientManager);
		Assert.assertTrue(null != _clusterManager);
		Assert.assertTrue(null != _diskManager);
		Assert.assertTrue(null != _consoleManager);
		
		// The design we use for the background thread is that it only responds to messages coming in from other threads.
		// A BlockingQueue of Runnables is used for this communication and the thread's loop is just to keep polling for
		// more elements until a global flag is cleared, causing it to terminate.
		// The Runnables are inner classes which are allowed full access to the NodeState's internal state.  Aside from
		// construction, and the queue, no other thread interacts with these state variables.
		// (note that the global running flag is modified by a command to shutdown).
		_keepRunning = true;
		while (_keepRunning) {
			// Poll for the next work item.
			Consumer<StateSnapshot> next = _commandQueue.blockingGet();
			// Create the state snapshot and pass it to the consumer.
			StateSnapshot snapshot = new StateSnapshot(_currentConfig.config, _lastCommittedMutationOffset, _nextGlobalMutationOffset-1, _lastCommittedEventOffset);
			next.accept(snapshot);
		}
	}

	public void registerClientManager(ClientManager clientManager) {
		// This MUST be called on the main thread.
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		// Input CANNOT be null.
		Assert.assertTrue(null != clientManager);
		// Reconfiguration is not defined.
		Assert.assertTrue(null == _clientManager);
		_clientManager = clientManager;
	}

	public void registerClusterManager(ClusterManager clusterManager) {
		// This MUST be called on the main thread.
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		// Input CANNOT be null.
		Assert.assertTrue(null != clusterManager);
		// Reconfiguration is not defined.
		Assert.assertTrue(null == _clusterManager);
		_clusterManager = clusterManager;
		
	}

	public void registerDiskManager(DiskManager diskManager) {
		// This MUST be called on the main thread.
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		// Input CANNOT be null.
		Assert.assertTrue(null != diskManager);
		// Reconfiguration is not defined.
		Assert.assertTrue(null == _diskManager);
		_diskManager = diskManager;
	}

	public void registerConsoleManager(ConsoleManager consoleManager) {
		// This MUST be called on the main thread.
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		// Input CANNOT be null.
		Assert.assertTrue(null != consoleManager);
		// Reconfiguration is not defined.
		Assert.assertTrue(null == _consoleManager);
		_consoleManager = consoleManager;
	}

	// <IClientManagerCallbacks>
	@Override
	public void ioEnqueueClientCommandForMainThread(Consumer<StateSnapshot> command) {
		// Called on an IO thread.
		Assert.assertTrue(Thread.currentThread() != _mainThread);
		_commandQueue.put(command);
	}

	@Override
	public long mainHandleValidClientMessage(UUID clientId, ClientMessage incoming) {
		// Called on main thread.
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		// All nonce accounting is done before we get here and acks are managed on response so just apply the message.
		// (we return the globalMutationOffset it was assigned so the caller can generate correct acks).
		return _mainNormalMessage(clientId, incoming);
	}

	@Override
	public void mainRequestMutationFetch(long mutationOffsetToFetch) {
		// Called on main thread.
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		// The mutations are 1-indexed so this must be a positive number.
		Assert.assertTrue(mutationOffsetToFetch > 0L);
		// It is invalid to request a mutation from the future.
		Assert.assertTrue(mutationOffsetToFetch < _nextGlobalMutationOffset);
		
		// We will fail to get the tuple if it has already been committed and is no longer in-flight.
		InFlightTuple tuple = _getInFlightTuple(mutationOffsetToFetch);
		if (null == tuple) {
			// If the mutation is on disk, fetch it from there.
			_diskManager.fetchMutation(mutationOffsetToFetch);
		} else {
			// This must be in our in-flight tuples waiting to commit.
			MutationRecord preCommitMutation = tuple.mutation;
			// This must be the mutation we wanted.
			Assert.assertTrue(mutationOffsetToFetch == preCommitMutation.globalOffset);
			// While it would be possible to specialize this path, we will keep the code smaller but handling this response asynchronously to hit the common path.
			_commandQueue.put(new Consumer<StateSnapshot>() {
				@Override
				public void accept(StateSnapshot arg) {
					Assert.assertTrue(Thread.currentThread() == _mainThread);
					_clientManager.mainReplayMutationForReconnects(arg, preCommitMutation, false);
				}});
		}
	}

	@Override
	public void mainRequestEventFetch(long nextLocalEventToFetch) {
		// Called on main thread.
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		Assert.assertTrue(nextLocalEventToFetch > 0L);
		_diskManager.fetchEvent(nextLocalEventToFetch);
	}
	// </IClientManagerCallbacks>

	// <IClusterManagerCallbacks>
	@Override
	public void ioEnqueueClusterCommandForMainThread(Consumer<StateSnapshot> command) {
		// Called on an IO thread.
		Assert.assertTrue(Thread.currentThread() != _mainThread);
		_commandQueue.put(command);
	}

	@Override
	public void mainConnectedToDownstreamPeer(ConfigEntry peer, long lastReceivedMutationOffset) {
		// Called on main thread.
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		// They can't be ahead of us until we worry about RAFT elections.
		Assert.assertTrue(lastReceivedMutationOffset < _nextGlobalMutationOffset);
		
		// See if we still have this peer (this will always be true in the current implementation as the ClusterManager checks this).
		DownstreamPeerState state = _unionOfDownstreamNodes.get(peer);
		
		// Set their state based on this information - we start up in a state of being ready to send them the next.
		state.isConnectionUp = true;
		state.isWritable = true;
		state.lastMutationOffsetReceived = lastReceivedMutationOffset;
		state.lastMutationOffsetSent = lastReceivedMutationOffset;
		state.nextMutationOffsetToSend = state.lastMutationOffsetReceived + 1;
		
		// See if this can be sent data now or request something to be fetched.
		_mainProcessDownstreamPeerIfReady(peer);
		
		// See if we can change our commit offset in light of this (we might have been waiting for them after a disconnect).
		_mainCommitValidInFlightTuples();
	}

	@Override
	public void mainDisconnectedFromDownstreamPeer(ConfigEntry peer) {
		// Called on main thread.
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		// See if we still have this peer (this will always be true in the current implementation as the ClusterManager checks this).
		DownstreamPeerState state = _unionOfDownstreamNodes.get(peer);
		state.isConnectionUp = false;
	}

	@Override
	public void mainUpstreamPeerConnected(ConfigEntry peer) {
		// Called on main thread.
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		
		// For now, we will interpret an upstream connection as the cluster leader.
		_currentState = RaftState.FOLLOWER;
		Assert.assertTrue(null == _clusterLeader);
		_clusterLeader = peer;
		// We start the leader as not writable since the ClusterManager just put it into this state.
		// (TODO:  Move this logic to ClusterManager)
		_leaderIsWritable = false;
		
		// Send all clients a REDIRECT.
		_clientManager.mainEnterFollowerState(_clusterLeader, _lastCommittedMutationOffset);
	}

	@Override
	public void mainUpstreamPeerDisconnected(ConfigEntry peer) {
		// Called on main thread.
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		// We currently don't do anything with these.
		System.out.println("Upstream peer disconnected: " + peer);
	}

	@Override
	public void mainDownstreamPeerWriteReady(ConfigEntry peer) {
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		
		// We need to set the flag in the DownstreamPeerState and see if this needs to write.
		DownstreamPeerState state = _unionOfDownstreamNodes.get(peer);
		// This can be null if the config changed.
		if (null != state) {
			Assert.assertTrue(!state.isWritable);
			state.isWritable = true;
			
			// See if this allows us to do something.
			_mainProcessDownstreamPeerIfReady(peer);
		}
	}

	@Override
	public void mainDownstreamPeerReceivedMutations(ConfigEntry peer, long lastReceivedMutationOffset) {
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		
		// Advance the state of the downstream node and see if it is ready to send or fetch.
		DownstreamPeerState state = _unionOfDownstreamNodes.get(peer);
		// This can be null if the config changed.
		if (null != state) {
			Assert.assertTrue(lastReceivedMutationOffset > state.lastMutationOffsetReceived );
			state.lastMutationOffsetReceived = lastReceivedMutationOffset;
			
			// For now, these are always lock-step but buffering may change that.
			Assert.assertTrue(state.lastMutationOffsetReceived == state.lastMutationOffsetSent);
			state.nextMutationOffsetToSend = state.lastMutationOffsetSent + 1;
			
			// See if this allows us to send them the next.
			_mainProcessDownstreamPeerIfReady(peer);
			
			// See if this updated our consensus commit offset such that we can proceed.
			_mainCommitValidInFlightTuples();
		}
	}

	@Override
	public void mainUpstreamPeerWriteReady(ConfigEntry peer) {
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		// For now, the only upstream peer is the leader.
		Assert.assertTrue(_clusterLeader == peer);
		Assert.assertTrue(!_leaderIsWritable);
		
		_leaderIsWritable = true;
		_checkAndAckToLeader();
	}

	@Override
	public void mainUpstreamSentMutation(ConfigEntry peer, MutationRecord record, long lastCommittedMutationOffset) {
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		// For now, the only upstream peer is the leader.
		Assert.assertTrue(_clusterLeader == peer);
		
		// Process this mutation and put it into our in-flight list.
		EventRecord event = null;
		switch (record.type) {
		case INVALID:
			throw Assert.unimplemented("Invalid message type");
		case TEMP:
			event = EventRecord.generateRecord(EventRecordType.TEMP, record.globalOffset, _nextLocalEventOffset++, record.clientId, record.clientNonce, record.payload);
			break;
		case UPDATE_CONFIG:
			// No record type for config.
			// We add this to the map of pending config changes so we will notify connected listeners when it commits.
			// We also fake the nodes in config since the follower is a speical-case until full raft is implemented.
			_configsPendingCommit.put(record.globalOffset, new SyncProgress(ClusterConfig.deserialize(record.payload), Collections.emptySet()));
			System.out.println("TODO: Handle config update on the follower");
			break;
		default:
			throw Assert.unreachable("Default record type");
		}
		// We actually store the mutation offset of the leader as the last one we "received" since the follower views this somewhat backward.
		_selfState.lastMutationOffsetReceived = lastCommittedMutationOffset;
		// This changes our consensus offset so re-run any commits.
		_mainCommitValidInFlightTuples();
		_nextGlobalMutationOffset = record.globalOffset + 1;
		_enqueueForCommit(record, event);
		
		_ackToSendToLeader = record.globalOffset;
		_checkAndAckToLeader();
	}
	// </IClusterManagerCallbacks>

	// <IDiskManagerBackgroundCallbacks>
	@Override
	public void mutationWasCommitted(MutationRecord completed) {
		// Called on an IO thread.
		Assert.assertTrue(Thread.currentThread() != _mainThread);
		_commandQueue.put(new Consumer<StateSnapshot>() {
			@Override
			public void accept(StateSnapshot arg) {
				Assert.assertTrue(Thread.currentThread() == _mainThread);
				// Update our global commit offset (set this first since other methods we are calling might want to read for common state).
				// We setup this commit so it must be sequential (this is a good check to make sure the commits aren't being re-ordered in the disk layer, too).
				Assert.assertTrue((arg.lastCommittedMutationOffset + 1) == completed.globalOffset);
				_lastCommittedMutationOffset = completed.globalOffset;
				// Only notify clients if we are the LEADER.
				if (RaftState.LEADER == _currentState) {
					_clientManager.mainProcessingPendingMessageCommits(completed.globalOffset);
				}
				
				// The mutation is only committed to disk when it is committed to the joint consensus so we can advance this now.
				SyncProgress newConfigProgress = _configsPendingCommit.remove(completed.globalOffset);
				if (null != newConfigProgress) {
					// We need a new snapshot since we just changed state in this command, above.
					StateSnapshot newSnapshot = new StateSnapshot(_currentConfig.config, _lastCommittedMutationOffset, _nextGlobalMutationOffset-1, _lastCommittedEventOffset);
					// This requires that we broadcast the config update to the connected clients and listeners.
					_clientManager.mainBroadcastConfigUpdate(newSnapshot, newConfigProgress.config);
					// We change the config but this would render the snapshot stale so we do it last, to make that clear.
					_currentConfig = newConfigProgress;
					// Note that only the leader currently worries about maintaining the downstream peers (we explicitly avoid making those connections until implementing RAFT).
					if (RaftState.LEADER == _currentState) {
						_rebuildDownstreamUnionAfterConfigChange();
					}
				}
			}});
	}

	@Override
	public void eventWasCommitted(EventRecord completed) {
		// Called on an IO thread.
		Assert.assertTrue(Thread.currentThread() != _mainThread);
		_commandQueue.put(new Consumer<StateSnapshot>() {
			@Override
			public void accept(StateSnapshot arg) {
				Assert.assertTrue(Thread.currentThread() == _mainThread);
				// We will eventually need to recor
				// Update our global commit offset (set this first since other methods we are calling might want to read for common state).
				// We setup this commit so it must be sequential (this is a good check to make sure the commits aren't being re-ordered in the disk layer, too).
				Assert.assertTrue((arg.lastCommittedEventOffset + 1) == completed.localOffset);
				_lastCommittedEventOffset = completed.localOffset;
				// See if any listeners want this.
				_clientManager.mainSendRecordToListeners(completed);
			}});
	}

	@Override
	public void mutationWasFetched(MutationRecord record) {
		// Called on an IO thread.
		Assert.assertTrue(Thread.currentThread() != _mainThread);
		_commandQueue.put(new Consumer<StateSnapshot>() {
			@Override
			public void accept(StateSnapshot arg) {
				Assert.assertTrue(Thread.currentThread() == _mainThread);
				
				// Check to see if a client needs this (mutations from disk are always committed)
				_clientManager.mainReplayMutationForReconnects(arg, record, true);
				
				// Check to see if a downstream peer needs this.
				_mainSendMutationToDownstreamPeers(record);
			}});
	}

	@Override
	public void eventWasFetched(EventRecord record) {
		// Called on an IO thread.
		Assert.assertTrue(Thread.currentThread() != _mainThread);
		_commandQueue.put(new Consumer<StateSnapshot>() {
			@Override
			public void accept(StateSnapshot arg) {
				Assert.assertTrue(Thread.currentThread() == _mainThread);
				// See what listeners requested this.
				_clientManager.mainSendRecordToListeners(record);
			}});
	}
	// </IDiskManagerBackgroundCallbacks>

	// <IConsoleManagerBackgroundCallbacks>
	@Override
	public void handleStopCommand() {
		// This MUST NOT be called on the main thread.
		Assert.assertTrue(Thread.currentThread() != _mainThread);
		
		_commandQueue.put(new Consumer<StateSnapshot>() {
			@Override
			public void accept(StateSnapshot arg) {
				Assert.assertTrue(Thread.currentThread() == _mainThread);
				_keepRunning = false;
			}});
	}
	// </IConsoleManagerBackgroundCallbacks>


	private long _mainNormalMessage(UUID clientId, ClientMessage incoming) {
		// Main thread helper.
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		// We return 0 if this is an error or the globalMutationOffset we assigned to the message so it can be acked.
		long globalMutationOffsetOfAcceptedMessage = 0L;
		switch (incoming.type) {
		case INVALID:
			Assert.unimplemented("Invalid message type");
			break;
		case TEMP: {
			// This is just for initial testing:  send the received, log it, and send the commit.
			byte[] contents = ((ClientMessagePayload_Temp)incoming.payload).contents;
			System.out.println("GOT TEMP FROM " + clientId + " nonce " + incoming.nonce + " data " + contents[0]);
			// Create the MutationRecord and EventRecord.
			long globalOffset = _getAndUpdateNextMutationOffset();
			long localOffset = _nextLocalEventOffset++;
			MutationRecord mutation = MutationRecord.generateRecord(MutationRecordType.TEMP, globalOffset, clientId, incoming.nonce, contents);
			EventRecord event = EventRecord.generateRecord(EventRecordType.TEMP, globalOffset, localOffset, clientId, incoming.nonce, contents);
			// Now request that both of these records be committed.
			_enqueueForCommit(mutation, event);
			globalMutationOffsetOfAcceptedMessage = globalOffset;
		}
			break;
		case POISON: {
			// This is just for initial testing:  send the received, log it, and send the commit.
			byte[] contents = ((ClientMessagePayload_Temp)incoming.payload).contents;
			System.out.println("GOT POISON FROM " + clientId + " nonce " + incoming.nonce + " data " + contents[0]);
			// Create the MutationRecord and EventRecord.
			long globalOffset = _getAndUpdateNextMutationOffset();
			long localOffset = _nextLocalEventOffset++;
			MutationRecord mutation = MutationRecord.generateRecord(MutationRecordType.TEMP, globalOffset, clientId, incoming.nonce, contents);
			EventRecord event = EventRecord.generateRecord(EventRecordType.TEMP, globalOffset, localOffset, clientId, incoming.nonce, contents);
			// Now request that both of these records be committed.
			_enqueueForCommit(mutation, event);
			
			// Now that we did the usual work, disconnect everyone.
			_clientManager.mainDisconnectAllClientsAndListeners();
			globalMutationOffsetOfAcceptedMessage = globalOffset;
		}
			break;
		case UPDATE_CONFIG: {
			// Eventually, this will kick-off the joint consensus where we change to having 2 active configs until this commits on all nodes and the local disk.
			// For now, however, we just send the received ack and enqueue this for commit (note that it DOES NOT generate an event - only a mutation).
			// The more complex operation happens after the commit completes since that is when we will change our state and broadcast the new config to all clients and listeners.
			ClusterConfig newConfig = ((ClientMessagePayload_UpdateConfig)incoming.payload).config;
			System.out.println("GOT UPDATE_CONFIG FROM " + clientId + ": " + newConfig.entries.length + " entries (nonce " + incoming.nonce + ")");
			
			// Create the MutationRecord but NO EventRecord.
			long globalOffset = _getAndUpdateNextMutationOffset();
			MutationRecord mutation = MutationRecord.generateRecord(MutationRecordType.UPDATE_CONFIG, globalOffset, clientId, incoming.nonce, newConfig.serialize());
			
			// Notes about handling a new config:
			// -we now enter (or compound) joint consensus, until this config commits on a majority of servers
			// -we need to find any new nodes and add them to our union of downstream peers
			// -we need to initiate an outgoing connection to any of these new nodes
			
			// Add the missing nodes and start the outgoing connections.
			Set<DownstreamPeerState> nodesInConfig = new HashSet<>();
			for (ConfigEntry entry : newConfig.entries) {
				DownstreamPeerState peer = _unionOfDownstreamNodes.get(entry);
				if (null == peer) {
					// This is a new node so start the connection and add it to the map.
					peer = new DownstreamPeerState();
					_clusterManager.mainOpenDownstreamConnection(entry);
					_unionOfDownstreamNodes.put(entry, peer);
				}
				nodesInConfig.add(peer);
			}
			// Add this to our pending map of commits so we know when to exit joint consensus.
			_configsPendingCommit.put(globalOffset, new SyncProgress(newConfig, nodesInConfig));
			
			// Request that the MutationRecord be committed (no EventRecord).
			_enqueueForCommit(mutation, null);
			globalMutationOffsetOfAcceptedMessage = globalOffset;
		}
			break;
		default:
			Assert.unimplemented("This is an invalid message for this client type and should be disconnected");
			break;
		}
		return globalMutationOffsetOfAcceptedMessage;
	}

	private void _enqueueForCommit(MutationRecord mutation, EventRecord event) {
		// Make sure the in-flight queue is consistent.
		Assert.assertTrue(mutation.globalOffset == (_inFlightMutations.size() + _inFlightMutationOffsetBias));
		
		// Make sure we aren't in a degenerate case where we can commit this, immediately (only applies to single-node clusters).
		long consensusOffset = _checkConsesusMutationOffset();
		if (mutation.globalOffset <= consensusOffset) {
			// Commit, immediately.
			Assert.assertTrue(_inFlightMutations.isEmpty());
			_commitAndUpdateBias(mutation, event);
		} else {
			// Store in list for later commit.
			_inFlightMutations.add(new InFlightTuple(mutation, event));
			// Notify anyone downstream about this.
			_mainSendMutationToDownstreamPeers(mutation);
		}
	}

	private long _checkConsesusMutationOffset() {
		// We want the minimum offset of all active configs.
		long offset = _currentConfig.checkCurrentProgress();
		for (SyncProgress pending : _configsPendingCommit.values()) {
			offset = Math.min(offset, pending.checkCurrentProgress());
		}
		return offset;
	}

	private long _getAndUpdateNextMutationOffset() {
		_selfState.lastMutationOffsetReceived = _nextGlobalMutationOffset;
		return _nextGlobalMutationOffset++;
	}

	private void _commitAndUpdateBias(MutationRecord mutation, EventRecord event) {
		// (note that the event is null for certain meta-messages like UPDATE_CONFIG).
		if (null != event) {
			_diskManager.commitEvent(event);
		}
		// TODO:  We probably want to lock-step the mutation on the event commit since we will be able to detect the broken data, that way, and replay it.
		_diskManager.commitMutation(mutation);
		// We are shifting the baseline by doing this.
		_inFlightMutationOffsetBias += 1;
	}

	private void _rebuildDownstreamUnionAfterConfigChange() {
		HashMap<ConfigEntry, DownstreamPeerState> copy = new HashMap<>(_unionOfDownstreamNodes);
		_unionOfDownstreamNodes.clear();
		for (ConfigEntry entry : _currentConfig.config.entries) {
			_unionOfDownstreamNodes.put(entry, copy.get(entry));
		}
		for (SyncProgress pending : _configsPendingCommit.values()) {
			for (ConfigEntry entry : pending.config.entries) {
				_unionOfDownstreamNodes.put(entry, copy.get(entry));
			}
		}
	}

	private void _mainCommitValidInFlightTuples() {
		long consensusOffset = _checkConsesusMutationOffset();
		while ((_inFlightMutationOffsetBias <= consensusOffset) && !_inFlightMutations.isEmpty()) {
			InFlightTuple record = _inFlightMutations.remove();
			_commitAndUpdateBias(record.mutation, record.event);
		}
	}

	private InFlightTuple _getInFlightTuple(long mutationOffsetToFetch) {
		Assert.assertTrue(mutationOffsetToFetch < _nextGlobalMutationOffset);
		
		InFlightTuple tuple = null;
		if (mutationOffsetToFetch >= _inFlightMutationOffsetBias) {
			int index = (int)(mutationOffsetToFetch - _inFlightMutationOffsetBias);
			tuple = _inFlightMutations.get(index);
		}
		return tuple;
	}

	private void _dispatchRequiredMutationFetch(long nextMutationOffsetToSend) {
		if (nextMutationOffsetToSend <= _lastCommittedMutationOffset) {
			// We need to fetch this mutation from disk but count how many are requesting it to ensure that we didn't
			// already request it (first one to need it issues the request).
			int countWaiting = 0;
			for (DownstreamPeerState elt : _unionOfDownstreamNodes.values()) {
				// If they are up, they need this mutation, and we didn't already send it to them, count them.
				if (elt.isConnectionUp
						&& elt.isWritable
						&& (elt.nextMutationOffsetToSend == nextMutationOffsetToSend)
						&& (elt.nextMutationOffsetToSend != elt.lastMutationOffsetSent)
				) {
					countWaiting += 1;
				}
			}
			if (1 == countWaiting) {
				// Only one peer is waiting so they are the first.
				_diskManager.fetchMutation(nextMutationOffsetToSend);
			}
		}
	}

	private void _mainSendMutationToDownstreamPeers(MutationRecord record) {
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		
		// Check all the peers and any who are ready can receive this, immediately.
		for (ConfigEntry peer : _unionOfDownstreamNodes.keySet()) {
			DownstreamPeerState state = _unionOfDownstreamNodes.get(peer);
			if (state.isConnectionUp
					&& state.isWritable
					&& (state.nextMutationOffsetToSend != state.lastMutationOffsetSent)
					&& (state.nextMutationOffsetToSend == record.globalOffset)
			) {
				_clusterManager.mainSendMutationToDownstreamNode(peer, record, _lastCommittedMutationOffset);
				state.lastMutationOffsetSent = state.nextMutationOffsetToSend;
				state.isWritable = false;
			}
		}
	}

	private void _mainProcessDownstreamPeerIfReady(ConfigEntry peer) {
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		
		DownstreamPeerState state = _unionOfDownstreamNodes.get(peer);
		if ((null != state)
				&& state.isConnectionUp
				&& state.isWritable
				&& (state.nextMutationOffsetToSend != state.lastMutationOffsetSent)
				&& (state.nextMutationOffsetToSend < _nextGlobalMutationOffset)
		) {
			// Check if we can just send them a pending mutation, right from memory.
			InFlightTuple inFlight = _getInFlightTuple(state.nextMutationOffsetToSend);
			if (null != inFlight) {
				// Send this to them, right away.
				_clusterManager.mainSendMutationToDownstreamNode(peer, inFlight.mutation, _lastCommittedMutationOffset);
				state.lastMutationOffsetSent = state.nextMutationOffsetToSend;
				state.isWritable = false;
			}
			
			if (null == inFlight) {
				// Check if we need to fetch any mutations to account for this (we don't want to double-fetch so this is a new requirement).
				_dispatchRequiredMutationFetch(state.nextMutationOffsetToSend);
			}
		}
	}

	private void _checkAndAckToLeader() {
		if (_leaderIsWritable && (0L != _ackToSendToLeader)) {
			_clusterManager.mainSendAckToLeader(_clusterLeader, _ackToSendToLeader);
			_leaderIsWritable = false;
			_ackToSendToLeader = 0L;
		}
	}


	private static class InFlightTuple {
		public final MutationRecord mutation;
		public final EventRecord event;
		
		public InFlightTuple(MutationRecord mutation, EventRecord event) {
			this.mutation = mutation;
			this.event = event;
		}
	}
}
