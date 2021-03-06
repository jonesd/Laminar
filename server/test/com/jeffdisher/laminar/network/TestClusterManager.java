package com.jeffdisher.laminar.network;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.util.UUID;
import java.util.function.Consumer;

import org.junit.Assert;
import org.junit.Test;

import com.jeffdisher.laminar.network.p2p.DownstreamMessage;
import com.jeffdisher.laminar.network.p2p.DownstreamPayload_AppendMutations;
import com.jeffdisher.laminar.network.p2p.DownstreamPayload_Identity;
import com.jeffdisher.laminar.network.p2p.UpstreamResponse;
import com.jeffdisher.laminar.state.StateSnapshot;
import com.jeffdisher.laminar.types.ClusterConfig;
import com.jeffdisher.laminar.types.ConfigEntry;
import com.jeffdisher.laminar.types.TopicName;
import com.jeffdisher.laminar.types.mutation.MutationRecord;
import com.jeffdisher.laminar.types.payload.Payload_KeyPut;
import com.jeffdisher.laminar.utils.TestingHelpers;


public class TestClusterManager {
	private static final int PORT_BASE = 3100;

	@Test
	public void testStartStop() throws Throwable {
		int port = PORT_BASE + 1;
		ConfigEntry self = _buildSelf();
		ServerSocketChannel socket = TestingHelpers.createServerSocket(port);
		TestClusterCallbacks callbacks = new TestClusterCallbacks();
		ClusterManager manager = new ClusterManager(self, socket, callbacks);
		manager.startAndWaitForReady();
		manager.stopAndWaitForTermination();
		socket.close();
	}

	/**
	 * Just verify that the ClusterManager can send outgoing connections.
	 * We will issue the connection request before binding the port to make sure that the retry works, too.
	 * Note that the TestClusterCallbacks are only notified of the connection when something happens on it so we will
	 * need to send a single data element over the connection.
	 */
	@Test
	public void testOutgoingConnections() throws Throwable {
		int managerPort = PORT_BASE + 2;
		int testPort = PORT_BASE + 3;
		ConfigEntry testEntry = new ConfigEntry(UUID.randomUUID(), new InetSocketAddress(testPort), new InetSocketAddress(9999));
		ConfigEntry self = _buildSelf();
		ServerSocketChannel socket = TestingHelpers.createServerSocket(managerPort);
		TestClusterCallbacks callbacks = new TestClusterCallbacks();
		ClusterManager manager = new ClusterManager(self, socket, callbacks);
		manager.startAndWaitForReady();
		
		// Issue the open connection request, wait for the command that we failed to run, and verify it wasn't connected.
		manager.mainOpenDownstreamConnection(testEntry);
		
		callbacks.runOneCommand();
		Assert.assertNull(callbacks.downstreamPeer);
		
		// Now, bind the port, process one command for the second failure, verify it isn't connected, and then another for the connection, and verify it was connected.
		ServerSocketChannel testSocket = TestingHelpers.createServerSocket(testPort);
		callbacks.runOneCommand();
		Assert.assertNull(callbacks.downstreamPeer);
		callbacks.runOneCommand();
		
		// Not connected until we receive this message and send our response.
		Assert.assertNull(callbacks.downstreamPeer);
		Socket fakePeerSocket = testSocket.accept().socket();
		ByteBuffer serverIdentity = ByteBuffer.wrap(TestingHelpers.readMessageInFrame(fakePeerSocket.getInputStream()));
		DownstreamMessage message = DownstreamMessage.deserializeFrom(serverIdentity);
		Assert.assertEquals(DownstreamMessage.Type.IDENTITY, message.type);
		Assert.assertEquals(self.nodeUuid, ((DownstreamPayload_Identity)message.payload).self.nodeUuid);
		long lastReceivedMutationOffset = 0L;
		UpstreamResponse response = UpstreamResponse.peerState(lastReceivedMutationOffset);
		ByteBuffer peerState = ByteBuffer.allocate(response.serializedSize());
		response.serializeInto(peerState);
		peerState.flip();
		TestingHelpers.writeMessageInFrame(fakePeerSocket.getOutputStream(), peerState.array());
		
		// Run the write-ready.
		callbacks.runOneCommand();
		// Before the read-ready (which will observe the peer state and try to start sync), we need to set up some data to sync.
		TopicName topic = TopicName.fromString("test");
		callbacks.nextMutationToReturn = MutationRecord.put(1L, 1L, topic, UUID.randomUUID(), 1L, new byte[0], new byte[] {1,2,3});
		// Now run the read-ready.
		callbacks.runOneCommand();
		
		// Read the message and send the ack.
		ByteBuffer mutation = ByteBuffer.wrap(TestingHelpers.readMessageInFrame(fakePeerSocket.getInputStream()));
		message = DownstreamMessage.deserializeFrom(mutation);
		Assert.assertEquals(DownstreamMessage.Type.APPEND_MUTATIONS, message.type);
		long mutationToAck = ((DownstreamPayload_AppendMutations)message.payload).lastCommittedMutationOffset;
		UpstreamResponse ack = UpstreamResponse.receivedMutations(mutationToAck);
		ByteBuffer ackBuffer = ByteBuffer.allocate(ack.serializedSize());
		ack.serializeInto(ackBuffer);
		ackBuffer.flip();
		TestingHelpers.writeMessageInFrame(fakePeerSocket.getOutputStream(), ackBuffer.array());
		
		// Run the write-ready, the read-ready and then check the peer was set.
		callbacks.runOneCommand();
		callbacks.runOneCommand();
		Assert.assertNotNull(callbacks.downstreamPeer);
		
		manager.stopAndWaitForTermination();
		fakePeerSocket.close();
		testSocket.close();
		socket.close();
	}

	/**
	 * Creates 2 ClusterManagers and verifies the messages being sent between them during and after an initial
	 * handshake which describes the need for 1 element to be synchronized from upstream to downstream.
	 */
	@Test
	public void testElementSynchronization() throws Throwable {
		int upstreamPort = PORT_BASE + 6;
		int downstreamPort = PORT_BASE + 7;
		ServerSocketChannel upstreamSocket = TestingHelpers.createServerSocket(upstreamPort);
		ServerSocketChannel downstreamSocket = TestingHelpers.createServerSocket(downstreamPort);
		ConfigEntry upstreamEntry = new ConfigEntry(UUID.randomUUID(), new InetSocketAddress(upstreamPort), new InetSocketAddress(9999));
		ConfigEntry downstreamEntry = new ConfigEntry(UUID.randomUUID(), new InetSocketAddress(downstreamPort), new InetSocketAddress(9999));
		TestClusterCallbacks upstreamCallbacks = new TestClusterCallbacks();
		TestClusterCallbacks downstreamCallbacks = new TestClusterCallbacks();
		ClusterManager upstreamManager = new ClusterManager(upstreamEntry, upstreamSocket, upstreamCallbacks);
		ClusterManager downstreamManager = new ClusterManager(downstreamEntry, downstreamSocket, downstreamCallbacks);
		upstreamManager.startAndWaitForReady();
		downstreamManager.startAndWaitForReady();
		
		// Initial handshake.
		TestingCommands.openConnectionAndSendIdentity(upstreamManager, upstreamCallbacks, downstreamEntry);
		TestingCommands.acceptConnectionAndSendState(downstreamManager, downstreamCallbacks);
		
		// When we run the readReady on upstreamCallbacks, it will try to fetch the mutation to send.
		long offset1 = 1L;
		UUID clientId1 = UUID.randomUUID();
		long nonce1 = 1L;
		byte[] payload1 = new byte[] {1,2,3};
		TopicName topic = TopicName.fromString("test");
		MutationRecord record1 = MutationRecord.put(1L, offset1, topic, clientId1, nonce1, new byte[0], payload1);
		TestingCommands.readPeerStateAndSendMutation(upstreamManager, upstreamCallbacks, record1);
		
		// We need to set the downstream to FOLLOWER state so that it can send acks - this normally happens in inside
		// the processing of the first message but we need to do it before it tries to ack so do it before.
		downstreamManager.mainEnterFollowerState();
		MutationRecord incoming = TestingCommands.readIncomingMutation(downstreamManager, downstreamCallbacks, upstreamEntry);
		Assert.assertEquals(offset1, incoming.globalOffset);
		
		// Running the readReady on upstream will observe the ack so we will see the callback.
		long ackReceived = TestingCommands.receiveAckFromDownstream(upstreamManager, upstreamCallbacks, downstreamEntry);
		Assert.assertEquals(offset1, ackReceived);
		
		downstreamManager.stopAndWaitForTermination();
		upstreamManager.stopAndWaitForTermination();
		downstreamSocket.close();
		upstreamSocket.close();
	}

	/**
	 * Creates 2 ClusterManagers and sends messages to 1 of them to force a term mismatch to verify that it acts as
	 * expected and properly continues running.
	 */
	@Test
	public void testTermMismatchInSync() throws Throwable {
		TopicName topic = TopicName.fromString("test");
		int upstreamPort = PORT_BASE + 8;
		int downstreamPort = PORT_BASE + 9;
		ServerSocketChannel upstreamSocket = TestingHelpers.createServerSocket(upstreamPort);
		ServerSocketChannel downstreamSocket = TestingHelpers.createServerSocket(downstreamPort);
		ConfigEntry upstreamEntry = new ConfigEntry(UUID.randomUUID(), new InetSocketAddress(upstreamPort), new InetSocketAddress(9999));
		ConfigEntry downstreamEntry = new ConfigEntry(UUID.randomUUID(), new InetSocketAddress(downstreamPort), new InetSocketAddress(9999));
		TestClusterCallbacks upstreamCallbacks = new TestClusterCallbacks();
		TestClusterCallbacks downstreamCallbacks = new TestClusterCallbacks();
		ClusterManager upstreamManager = new ClusterManager(upstreamEntry, upstreamSocket, upstreamCallbacks);
		ClusterManager downstreamManager = new ClusterManager(downstreamEntry, downstreamSocket, downstreamCallbacks);
		upstreamManager.startAndWaitForReady();
		downstreamManager.startAndWaitForReady();
		
		// Initial handshake.
		TestingCommands.openConnectionAndSendIdentity(upstreamManager, upstreamCallbacks, downstreamEntry);
		TestingCommands.acceptConnectionAndSendState(downstreamManager, downstreamCallbacks);
		
		// We need to set the downstream to FOLLOWER state so that it can send acks - this normally happens in inside
		// the processing of the first message but we need to do it before it tries to ack so do it before.
		downstreamManager.mainEnterFollowerState();
		MutationRecord record1 = MutationRecord.put(1L, 1L, topic, UUID.randomUUID(), 1L, new byte[0], new byte[] {1,2,3});
		TestingCommands.readPeerStateAndSendMutation(upstreamManager, upstreamCallbacks, record1);
		
		Assert.assertNull(downstreamCallbacks.upstreamPeer);
		Assert.assertEquals(0L, downstreamCallbacks.upstreamCommitOffset);
		MutationRecord incoming = TestingCommands.readIncomingMutation(downstreamManager, downstreamCallbacks, upstreamEntry);
		Assert.assertNotNull(incoming);
		
		// Running the readReady on upstream will observe the ack so we will see the callback.
		long receivedAckNumber = TestingCommands.receiveAckFromDownstream(upstreamManager, upstreamCallbacks, downstreamEntry);
		Assert.assertEquals(1L, receivedAckNumber);
		
		// Now, send another message which will invalidate the term number of the last one we sent.
		MutationRecord record2 = MutationRecord.put(2L, 2L, topic, record1.clientId, 2L, new byte[0], new byte[] {2});
		// (this 2L is what should cause the downstream to drop record1).
		TestingCommands.sendNewMutation(upstreamManager, upstreamCallbacks, new StateSnapshot(null, 0L, 0L, 2L), 2L, record2);
		// Receiving this will result in a PEER_STATE response.
		incoming = TestingCommands.readIncomingMutation(downstreamManager, downstreamCallbacks, upstreamEntry);
		Assert.assertNull(incoming);
		
		// Upstream now receives the PEER_STATE and tries to restart so set up the corrected mutation (term is still 0).
		MutationRecord record1_fix = MutationRecord.put(2L, record1.globalOffset, topic, record1.clientId, record1.clientNonce, new byte[0], new byte[] {1,2,3, 4, 5, 6});
		TestingCommands.readPeerStateAndSendMutation(upstreamManager, upstreamCallbacks, record1_fix);
		incoming = TestingCommands.readIncomingMutation(downstreamManager, downstreamCallbacks, upstreamEntry);
		Assert.assertArrayEquals(((Payload_KeyPut)record1_fix.payload).key, ((Payload_KeyPut)incoming.payload).key);
		Assert.assertArrayEquals(((Payload_KeyPut)record1_fix.payload).value, ((Payload_KeyPut)incoming.payload).value);
		
		// Upstream receives the ack and tries to send the next mutation so give them the last one we sent, new term.
		receivedAckNumber = TestingCommands.receiveAckAndSend(upstreamManager, upstreamCallbacks, record1_fix.termNumber, record2);
		Assert.assertEquals(record1_fix.globalOffset, receivedAckNumber);
		
		incoming = TestingCommands.readIncomingMutation(downstreamManager, downstreamCallbacks, upstreamEntry);
		Assert.assertArrayEquals(((Payload_KeyPut)record2.payload).key, ((Payload_KeyPut)incoming.payload).key);
		Assert.assertArrayEquals(((Payload_KeyPut)record2.payload).value, ((Payload_KeyPut)incoming.payload).value);
		receivedAckNumber = TestingCommands.receiveAckFromDownstream(upstreamManager, upstreamCallbacks, downstreamEntry);
		Assert.assertEquals(record2.globalOffset, receivedAckNumber);
		
		downstreamManager.stopAndWaitForTermination();
		upstreamManager.stopAndWaitForTermination();
		downstreamSocket.close();
		upstreamSocket.close();
	}

	/**
	 * Creates 2 ClusterManagers and demonstrates what happens if the upstream attempts to send 2 mutations before the
	 * downstream can ack either of them.
	 * We expect that only the first will be sent since the upstream should lock-step on the mutation acks before
	 * sending another.  It will do this by deciding that the downstream wasn't yet available to receive the mutation
	 * being offered.
	 * Reason why this is important:  The state machines related to how both sides synchronize is not currently able to
	 * handle multiple in-flight in the case where they are sending contradictory information:  move forward and move
	 * backward.  We use this test to verify such cases won't be introduced until the state machine logic is explicitly
	 * changed to handle those situations.
	 */
	@Test
	public void testSendTooQuickly() throws Throwable {
		TopicName topic = TopicName.fromString("test");
		int upstreamPort = PORT_BASE + 10;
		int downstreamPort = PORT_BASE + 11;
		ServerSocketChannel upstreamSocket = TestingHelpers.createServerSocket(upstreamPort);
		ServerSocketChannel downstreamSocket = TestingHelpers.createServerSocket(downstreamPort);
		ConfigEntry upstreamEntry = new ConfigEntry(UUID.randomUUID(), new InetSocketAddress(upstreamPort), new InetSocketAddress(9999));
		ConfigEntry downstreamEntry = new ConfigEntry(UUID.randomUUID(), new InetSocketAddress(downstreamPort), new InetSocketAddress(9999));
		TestClusterCallbacks upstreamCallbacks = new TestClusterCallbacks();
		TestClusterCallbacks downstreamCallbacks = new TestClusterCallbacks();
		ClusterManager upstreamManager = new ClusterManager(upstreamEntry, upstreamSocket, upstreamCallbacks);
		ClusterManager downstreamManager = new ClusterManager(downstreamEntry, downstreamSocket, downstreamCallbacks);
		upstreamManager.startAndWaitForReady();
		downstreamManager.startAndWaitForReady();
		
		// Initial handshake.
		TestingCommands.openConnectionAndSendIdentity(upstreamManager, upstreamCallbacks, downstreamEntry);
		TestingCommands.acceptConnectionAndSendState(downstreamManager, downstreamCallbacks);
		
		// Provide a mutation in direct response to the PEER_STATE.
		downstreamManager.mainEnterFollowerState();
		MutationRecord record1 = MutationRecord.put(1L, 1L, topic, UUID.randomUUID(), 1L, new byte[0], new byte[] {1,2,3});
		TestingCommands.readPeerStateAndSendMutation(upstreamManager, upstreamCallbacks, record1);
		
		// Process the mutation on the downstream, which should result in an ack being sent back.
		MutationRecord received = TestingCommands.readIncomingMutation(downstreamManager, downstreamCallbacks, upstreamEntry);
		Assert.assertEquals(record1.globalOffset, received.globalOffset);
		
		// Immediately send another mutation before accepting the ack and observe that it FAILS to send (see the failSendNewMutation command).
		MutationRecord record2 = MutationRecord.put(1L, 2L, topic, record1.clientId, 2L, new byte[0], new byte[] {2});
		TestingCommands.failSendNewMutation(upstreamManager, upstreamCallbacks, new StateSnapshot(null, 0L, 0L, 2L), 1L, record2);
		
		// Process the ack on the upstream and demonstrate that it now wants to send the message it failed to send.
		long receivedMutation = TestingCommands.receiveAckFromDownstream(upstreamManager, upstreamCallbacks, downstreamEntry);
		Assert.assertEquals(record1.globalOffset, receivedMutation);
		TestingCommands.sendNewMutation(upstreamManager, upstreamCallbacks, new StateSnapshot(null, 0L, 0L, 2L), 1L, record2);
		
		downstreamManager.stopAndWaitForTermination();
		upstreamManager.stopAndWaitForTermination();
		downstreamSocket.close();
		upstreamSocket.close();
	}


	private static ConfigEntry _buildSelf() throws UnknownHostException {
		InetAddress localhost = InetAddress.getLocalHost();
		InetSocketAddress cluster = ClusterConfig.cleanSocketAddress(new InetSocketAddress(localhost, 1000));
		InetSocketAddress client = ClusterConfig.cleanSocketAddress(new InetSocketAddress(localhost, 1001));
		return new ConfigEntry(UUID.randomUUID(), cluster, client);
	}


	private static class TestClusterCallbacks implements IClusterManagerCallbacks {
		private Consumer<StateSnapshot> _command;
		public ConfigEntry downstreamPeer;
		public ConfigEntry upstreamPeer;
		public MutationRecord upstreamMutation;
		public long upstreamCommitOffset;
		public long downstreamReceivedMutation;
		public long nextPreviousTermNumberToReturn;
		public MutationRecord nextMutationToReturn;
		private long _previousTermNumber;
		
		public synchronized void runOneCommand() throws InterruptedException {
			while (null == _command) {
				this.wait();
			}
			_command.accept(new StateSnapshot(null, 0L, 0L, 1L));
			_command = null;
			this.notifyAll();
		}
		
		@Override
		public void ioEnqueueClusterCommandForMainThread(Consumer<StateSnapshot> command) {
			_blockToStoreCommand(command);
		}
		
		@Override
		public void ioEnqueuePriorityClusterCommandForMainThread(Consumer<StateSnapshot> command, long delayMillis) {
			// For the purposes of the test, we just treat this like a normal command.
			_blockToStoreCommand(command);
		}
		
		@Override
		public void mainEnqueuePriorityClusterCommandForMainThread(Consumer<StateSnapshot> command, long delayMillis) {
			// WARNING:  We explicitly drop this for our current tests.
			// Part of this is because it would require changing the message hand-off (this is a reentrant call so it breaks this locked single hand-off design).
			// The other part is that we know that this is used for scheduling heartbeats which we don't currently test and relies on wall clock time.
			// TODO:  Redesign this along with a way to inject a time source to ClusterManager.
		}
		
		@Override
		public long mainAppendMutationFromUpstream(ConfigEntry peer, long upstreamTermNumber, long previousMutationTermNumber, MutationRecord mutation) {
			if (null == this.upstreamPeer) {
				this.upstreamPeer = peer;
			} else {
				Assert.assertTrue(this.upstreamPeer == peer);
			}
			// For now, to make the revert on term number mismatch test pass, we will revert to previous term 0 whenever we see offset 1.
			if (1L == mutation.globalOffset) {
				_previousTermNumber = 0L;
			}
			long mutationToFetchNext = -1L;
			if (_previousTermNumber == previousMutationTermNumber) {
				Assert.assertNull(this.upstreamMutation);
				Assert.assertEquals(0L,  this.upstreamCommitOffset);
				this.upstreamMutation = mutation;
				_previousTermNumber = mutation.termNumber;
				// This is good so fetch the next.
				mutationToFetchNext = mutation.globalOffset + 1;
			} else {
				// This is an inconsistency so we want the one before the one they sent us.
				mutationToFetchNext = mutation.globalOffset - 1;
			}
			return mutationToFetchNext;
		}
		
		@Override
		public void mainCommittedMutationOffsetFromUpstream(ConfigEntry peer, long upstreamTermNumber, long lastCommittedMutationOffset) {
			this.upstreamCommitOffset = lastCommittedMutationOffset;
		}
		
		@Override
		public IClusterManagerCallbacks.MutationWrapper mainClusterFetchMutationIfAvailable(long mutationOffset) {
			IClusterManagerCallbacks.MutationWrapper wrapperToReturn = null;
			if (null != this.nextMutationToReturn) {
				Assert.assertEquals(this.nextMutationToReturn.globalOffset, mutationOffset);
				wrapperToReturn = new IClusterManagerCallbacks.MutationWrapper(this.nextPreviousTermNumberToReturn, this.nextMutationToReturn);
				this.nextMutationToReturn = null;
			}
			return wrapperToReturn;
		}
		
		@Override
		public void mainReceivedAckFromDownstream(ConfigEntry peer, long mutationOffset) {
			if (null == this.downstreamPeer) {
				this.downstreamPeer = peer;
			} else {
				Assert.assertTrue(this.downstreamPeer == peer);
			}
			Assert.assertEquals(0L,  this.downstreamReceivedMutation);
			this.downstreamReceivedMutation = mutationOffset;
		}
		
		private synchronized void _blockToStoreCommand(Consumer<StateSnapshot> command) {
			while (null != _command) {
				try {
					this.wait();
				} catch (InterruptedException e) {
					// We don't use interruption in this test - this is just for lock-step connection testing.
					Assert.fail(e.getLocalizedMessage());
				}
			}
			_command = command;
			this.notifyAll();
		}

		@Override
		public boolean mainReceivedRequestForVotes(ConfigEntry peer, long newTermNumber, long candidateLastReceivedMutationTerm, long candidateLastReceivedMutation) {
			Assert.fail("Not used");
			return false;
		}

		@Override
		public void mainReceivedVoteFromFollower(ConfigEntry peer, long newTermNumber) {
			Assert.fail("Not used");
		}

		@Override
		public void mainUpstreamMessageDidTimeout() {
			Assert.fail("Not used");
		}
	}


	private static class TestingCommands {
		public static void openConnectionAndSendIdentity(ClusterManager manager, TestClusterCallbacks callbacks, ConfigEntry peer) throws InterruptedException {
			manager.mainOpenDownstreamConnection(peer);
			// -1 outboundNodeConnected (triggers send of IDENTITY)
			callbacks.runOneCommand();
			// -1 nodeWriteReady
			callbacks.runOneCommand();
			
		}
		public static long receiveAckFromDownstream(ClusterManager manager, TestClusterCallbacks callbacks, ConfigEntry downstreamEntry) throws InterruptedException {
			Assert.assertEquals(0L, callbacks.downstreamReceivedMutation);
			Assert.assertNull(callbacks.downstreamPeer);
			// -1 nodeReadReady (provides callback).
			callbacks.runOneCommand();
			Assert.assertEquals(downstreamEntry, callbacks.downstreamPeer);
			long toReturn = callbacks.downstreamReceivedMutation;
			callbacks.downstreamReceivedMutation = 0L;
			callbacks.downstreamPeer = null;
			return toReturn;
		}
		public static long receiveAckAndSend(ClusterManager manager, TestClusterCallbacks callbacks, long previousMutationNumber, MutationRecord record) throws InterruptedException {
			Assert.assertEquals(0L, callbacks.downstreamReceivedMutation);
			callbacks.nextMutationToReturn = record;
			callbacks.nextPreviousTermNumberToReturn = previousMutationNumber;
			// -1 nodeReadReady (the receive ack and send the new mutation).
			callbacks.runOneCommand();
			// -1 nodeWriteReady.
			callbacks.runOneCommand();
			long toReturn = callbacks.downstreamReceivedMutation;
			callbacks.downstreamReceivedMutation = 0L;
			callbacks.downstreamPeer = null;
			return toReturn;
		}
		public static void acceptConnectionAndSendState(ClusterManager manager, TestClusterCallbacks callbacks) throws InterruptedException {
			// -2 nodeDidConnect
			callbacks.runOneCommand();
			// -2 nodeReadReady (reads IDENTITY and sends PEER_STATE)
			callbacks.runOneCommand();
			// -2 nodeWriteReady
			callbacks.runOneCommand();
		}
		public static void readPeerStateAndSendMutation(ClusterManager manager, TestClusterCallbacks callbacks, MutationRecord record) throws InterruptedException {
			// When we run the readReady on upstreamCallbacks, it will try to fetch the mutation to send.
			callbacks.nextMutationToReturn = record;
			// -1 nodeReadReady (reads PEER_STATE - picks up mutation - sends APPEND).
			callbacks.runOneCommand();
			Assert.assertNull(callbacks.nextMutationToReturn);
			// -1 nodeWriteReady
			callbacks.runOneCommand();
		}
		public static MutationRecord readIncomingMutation(ClusterManager manager, TestClusterCallbacks callbacks, ConfigEntry expectedSender) throws InterruptedException {
			// Running readReady on downstream callbacks will give us the mutation so we will see who this is.
			// -2 nodeReadReady (reads APPEND - provides callback - sends ACK or PEER_STATE).
			Assert.assertNull(callbacks.upstreamMutation);
			callbacks.runOneCommand();
			MutationRecord toReturn = callbacks.upstreamMutation;
			callbacks.upstreamMutation = null;
			// -2 nodeWriteReady
			callbacks.runOneCommand();
			Assert.assertEquals(expectedSender.nodeUuid, callbacks.upstreamPeer.nodeUuid);
			return toReturn;
		}
		public static void sendNewMutation(ClusterManager manager, TestClusterCallbacks callbacks, StateSnapshot stateSnapshot, long previousMutationNumber, MutationRecord record) throws InterruptedException {
			boolean didSend = manager.mainMutationWasReceivedOrFetched(stateSnapshot, previousMutationNumber, record);
			Assert.assertTrue(didSend);
			// -1 nodeWriteReady
			callbacks.runOneCommand();
		}
		public static void failSendNewMutation(ClusterManager manager, TestClusterCallbacks callbacks, StateSnapshot stateSnapshot, long previousMutationNumber, MutationRecord record) throws InterruptedException {
			boolean didSend = manager.mainMutationWasReceivedOrFetched(stateSnapshot, previousMutationNumber, record);
			Assert.assertFalse(didSend);
			// No waiting for write-ready since we didn't send.
		}
	}
}
