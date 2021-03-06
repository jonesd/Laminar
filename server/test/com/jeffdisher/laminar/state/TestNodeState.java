package com.jeffdisher.laminar.state;

import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import org.junit.Assert;
import org.junit.Test;

import com.jeffdisher.laminar.console.IConsoleManager;
import com.jeffdisher.laminar.disk.CommittedMutationRecord;
import com.jeffdisher.laminar.network.IClusterManagerCallbacks;
import com.jeffdisher.laminar.types.ClusterConfig;
import com.jeffdisher.laminar.types.CommitInfo;
import com.jeffdisher.laminar.types.ConfigEntry;
import com.jeffdisher.laminar.types.TopicName;
import com.jeffdisher.laminar.types.event.EventRecord;
import com.jeffdisher.laminar.types.message.ClientMessage;
import com.jeffdisher.laminar.types.mutation.MutationRecord;
import com.jeffdisher.laminar.types.payload.Payload_KeyPut;


/**
 * Unit tests for NodeState:  The core decision-making component of Laminar.
 */
public class TestNodeState {
	/**
	 * Tests that we can immediately stop the NodeState after starting it.
	 */
	@Test
	public void testStop() throws Throwable {
		MainThread test = new MainThread();
		test.start();
		test.startLatch.await();
		Runner runner = new Runner(test.nodeState);
		runner.runVoid((snapshot) -> test.nodeState.mainHandleStopCommand());
		test.join();
	}

	/**
	 * Tests a synthesized client sending a new mutation.  The lifecycle of this mutation is followed through commit
	 * and acknowledgement to client.
	 */
	@Test
	public void testOneClientCommit() throws Throwable {
		TopicName topic = TopicName.fromString("fake");
		MainThread test = new MainThread();
		test.start();
		test.startLatch.await();
		Runner runner = new Runner(test.nodeState);
		
		// Register the topic and say it was committed.
		F<CommittedMutationRecord> preMutation = test.diskManager.get_commitMutation();
		long mutationNumber = runner.run((snapshot) -> test.nodeState.mainHandleValidClientMessage(UUID.randomUUID(), ClientMessage.createTopic(1L, topic, new byte[0], new byte[0])));
		Assert.assertEquals(1L, mutationNumber);
		runner.runVoid((snapshot) -> test.nodeState.mainMutationWasCommitted(preMutation.get()));
		
		// Send the ClientMessage.
		F<CommittedMutationRecord> mutation = test.diskManager.get_commitMutation();
		F<EventRecord> event = test.diskManager.get_commitEvent();
		mutationNumber = runner.run((snapshot) -> test.nodeState.mainHandleValidClientMessage(UUID.randomUUID(), ClientMessage.put(2L, topic, new byte[0], new byte[] {1})));
		Assert.assertEquals(2L, mutationNumber);
		Assert.assertEquals(mutationNumber, mutation.get().record.globalOffset);
		Assert.assertEquals(mutationNumber, event.get().globalOffset);
		
		// Say the corresponding mutation was committed.
		F<Long> toClient = test.clientManager.get_mainProcessingPendingMessageCommits();
		F<Long> toCluster = test.clusterManager.get_mainMutationWasCommitted();
		runner.runVoid((snapshot) -> test.nodeState.mainMutationWasCommitted(mutation.get()));
		Assert.assertEquals(mutationNumber, toClient.get().longValue());
		Assert.assertEquals(mutationNumber, toCluster.get().longValue());
		
		// Stop.
		runner.runVoid((snapshot) -> test.nodeState.mainHandleStopCommand());
		test.join();
	}

	/**
	 * Tests that in-flight messages with a mismatching term number are removed and the sync state is restarted when
	 * they are detected.
	 */
	@Test
	public void testDropInFlightOnTermMismatch() throws Throwable {
		MainThread test = new MainThread();
		test.start();
		test.startLatch.await();
		Runner runner = new Runner(test.nodeState);
		TopicName topic = TopicName.fromString("fake");
		ConfigEntry upstream = new ConfigEntry(UUID.randomUUID(), new InetSocketAddress(3), new InetSocketAddress(4));
		MutationRecord record1 = MutationRecord.put(1L, 1L, topic, UUID.randomUUID(), 1, new byte[0], new byte[] {1});
		MutationRecord record2 = MutationRecord.put(2L, 2L, topic, UUID.randomUUID(), 1, new byte[0], new byte[] {2});
		MutationRecord record1_fix = MutationRecord.put(2L, 1L, record1.topic, record1.clientId, record1.clientNonce, ((Payload_KeyPut)record1.payload).key, ((Payload_KeyPut)record1.payload).value);
		
		// Send the initial message.
		F<Long> client_mainEnterFollowerState = test.clientManager.get_mainEnterFollowerState();
		F<Void> cluster_mainEnterFollowerState = test.clusterManager.get_mainEnterFollowerState();
		F<MutationRecord> mainMutationWasReceivedOrFetched = test.clusterManager.get_mainMutationWasReceivedOrFetched();
		long nextToLoad = runner.run((snapshot) -> test.nodeState.mainAppendMutationFromUpstream(upstream, 1L, 0L, record1));
		Assert.assertEquals(record1.globalOffset + 1, nextToLoad);
		Assert.assertEquals(0L, client_mainEnterFollowerState.get().longValue());
		Assert.assertEquals(record1, mainMutationWasReceivedOrFetched.get());
		cluster_mainEnterFollowerState.get();
		// Send a message which contradicts that.
		// (note that the contradiction doesn't send mainMutationWasReceivedOrFetched)
		nextToLoad = runner.run((snapshot) -> test.nodeState.mainAppendMutationFromUpstream(upstream, 2L, 2L, record2));
		Assert.assertEquals(record2.globalOffset - 1, nextToLoad);
		// Send a replacement message.
		mainMutationWasReceivedOrFetched = test.clusterManager.get_mainMutationWasReceivedOrFetched();
		nextToLoad = runner.run((snapshot) -> test.nodeState.mainAppendMutationFromUpstream(upstream, 2L, 0L, record1_fix));
		Assert.assertEquals(record1_fix.globalOffset + 1, nextToLoad);
		Assert.assertEquals(record1_fix, mainMutationWasReceivedOrFetched.get());
		// Re-send the failure.
		mainMutationWasReceivedOrFetched = test.clusterManager.get_mainMutationWasReceivedOrFetched();
		nextToLoad = runner.run((snapshot) -> test.nodeState.mainAppendMutationFromUpstream(upstream, 2L, 2L, record2));
		Assert.assertEquals(record2.globalOffset + 1, nextToLoad);
		Assert.assertEquals(record2, mainMutationWasReceivedOrFetched.get());
		
		// Stop.
		runner.runVoid((snapshot) -> test.nodeState.mainHandleStopCommand());
		test.join();
	}

	/**
	 * Tests that a leader will not commit mutations from a previous term until it can commit something from its current
	 * term.
	 * This is essentially a test of the behaviour described in section 5.4.2 of the Raft paper.
	 * This means populating a node with mutations from upstream, only committing some of them, forcing the node to
	 * become the leader, asking it to send the received mutations in its cache downstream, sending it all the acks,
	 * verifying that none of them have committed yet, creating a new mutation on that node, sending it downstream,
	 * receiving the acks, and then committing all the mutations at that time.
	 */
	@Test
	public void testWaitingForNewTermCommit() throws Throwable {
		// Create the node.
		MainThread test = new MainThread();
		test.start();
		test.startLatch.await();
		NodeState nodeState = test.nodeState;
		Runner runner = new Runner(nodeState);
		TopicName topic = TopicName.fromString("fake");
		ConfigEntry originalEntry = test.initialConfig.entries[0];
		ConfigEntry upstreamEntry = new ConfigEntry(UUID.randomUUID(), new InetSocketAddress(3), new InetSocketAddress(4));
		ClusterConfig newConfig = ClusterConfig.configFromEntries(new ConfigEntry[] {originalEntry, upstreamEntry});
		// Send it 2 mutations (first one is 2-node config).
		MutationRecord configChangeRecord = MutationRecord.updateConfig(1L, 1L, UUID.randomUUID(), 1L, newConfig);
		// (note that this will cause them to become a FOLLOWER).
		long nextToLoad = runner.run((snapshot) -> nodeState.mainAppendMutationFromUpstream(upstreamEntry, 1L, 0L, configChangeRecord));
		Assert.assertEquals(configChangeRecord.globalOffset + 1, nextToLoad);
		MutationRecord tempRecord = MutationRecord.put(1L, 2L, topic, UUID.randomUUID(), 1L, new byte[0], new byte[] {1});
		nextToLoad = runner.run((snapshot) -> nodeState.mainAppendMutationFromUpstream(upstreamEntry, 1L, 1L, tempRecord));
		Assert.assertEquals(tempRecord.globalOffset + 1, nextToLoad);
		// Tell it the first mutation committed (meaning that config will be active).
		runner.runVoid((snapshot) -> nodeState.mainCommittedMutationOffsetFromUpstream(upstreamEntry, 1L, 1L));
		
		// <election>
		// Force it to enter CANDIDATE state.
		F<Long> electionStart = test.clusterManager.get_mainEnterCandidateState();
		runner.runVoid((snapshot) -> nodeState.mainForceLeader());
		Assert.assertEquals(2L, electionStart.get().longValue());
		// Send back vote (allows it to enter LEADER state).
		F<Void> electionEnd = test.clusterManager.get_mainEnterLeaderState();
		runner.runVoid((snapshot) -> nodeState.mainReceivedVoteFromFollower(upstreamEntry, 2L));
		electionEnd.get();
		// </election>
		
		// Ask it to send the remaining mutation downstream.
		IClusterManagerCallbacks.MutationWrapper wrapper = runner.run((snapshot) -> nodeState.mainClusterFetchMutationIfAvailable(2L));
		Assert.assertNotNull(wrapper);
		// Send it the ack for the mutation.
		runner.runVoid((snapshot) -> nodeState.mainReceivedAckFromDownstream(upstreamEntry, 2L));
		// Verify that mutation 2 still hasn't committed (we do that by trying to fetch it, inline - committed mutations need to be fetched).
		MutationRecord mutation = runner.run((snapshot) -> nodeState.mainClientFetchMutationIfAvailable(2L));
		Assert.assertEquals(tempRecord, mutation);
		// Create new mutation (3).
		ClientMessage newTemp = ClientMessage.put(1L, TopicName.fromString("fake"), new byte[0], new byte[]{2});
		long mutationOffset = runner.run((snapshot) -> nodeState.mainHandleValidClientMessage(UUID.randomUUID(), newTemp));
		Assert.assertEquals(3L, mutationOffset);
		// Ask it to send the new mutation downstream.
		wrapper = runner.run((snapshot) -> nodeState.mainClusterFetchMutationIfAvailable(3L));
		Assert.assertEquals(1L, wrapper.previousMutationTermNumber);
		Assert.assertEquals(2L, wrapper.record.termNumber);
		// Send it the ack for the new mutation (this causes it to immediately commit mutations 2 and 3).
		F<CommittedMutationRecord> commit1 = test.diskManager.get_commitMutation();
		F<CommittedMutationRecord> commit2 = test.diskManager.get_commitMutation();
		runner.runVoid((snapshot) -> nodeState.mainReceivedAckFromDownstream(upstreamEntry, 3L));
		Assert.assertEquals(2L, commit1.get().record.globalOffset);
		Assert.assertEquals(3L, commit2.get().record.globalOffset);
		
		// Verify all mutations are committed.
		Assert.assertNull(runner.run((snapshot) -> nodeState.mainClusterFetchMutationIfAvailable(1L)));
		Assert.assertNull(runner.run((snapshot) -> nodeState.mainClusterFetchMutationIfAvailable(2L)));
		Assert.assertNull(runner.run((snapshot) -> nodeState.mainClusterFetchMutationIfAvailable(3L)));
		
		// Stop.
		runner.runVoid((snapshot) -> test.nodeState.mainHandleStopCommand());
		test.join();
	}

	@Test
	public void testSingleVotePerElection() throws Throwable {
		// Create the node.
		MainThread test = new MainThread();
		test.start();
		test.startLatch.await();
		NodeState nodeState = test.nodeState;
		Runner runner = new Runner(nodeState);
		ConfigEntry upstreamEntry1 = new ConfigEntry(UUID.randomUUID(), new InetSocketAddress(3), new InetSocketAddress(4));
		ConfigEntry upstreamEntry2 = new ConfigEntry(UUID.randomUUID(), new InetSocketAddress(5), new InetSocketAddress(6));
		
		// We should see the first call but NOT the second.
		F<Void> callFollower = test.clusterManager.get_mainEnterFollowerState();
		runner.runVoid((snapshot) -> nodeState.mainReceivedRequestForVotes(upstreamEntry1, 2L, 1L, 1L));
		callFollower.get();
		callFollower = test.clusterManager.get_mainEnterFollowerState();
		runner.runVoid((snapshot) -> nodeState.mainReceivedRequestForVotes(upstreamEntry2, 2L, 1L, 1L));
		Assert.assertFalse(callFollower.pollDidCall());
	}

	@Test
	public void testStartElectionOnVoteRequest() throws Throwable {
		// Create the node.
		MainThread test = new MainThread();
		test.start();
		test.startLatch.await();
		NodeState nodeState = test.nodeState;
		Runner runner = new Runner(nodeState);
		
		// Send in a mutation so we have something in storage and so the node knows that there it has downstream peers.
		ConfigEntry originalEntry = test.initialConfig.entries[0];
		ConfigEntry upstreamEntry1 = new ConfigEntry(UUID.randomUUID(), new InetSocketAddress(3), new InetSocketAddress(4));
		ClusterConfig newConfig = ClusterConfig.configFromEntries(new ConfigEntry[] {originalEntry, upstreamEntry1});
		
		F<CommittedMutationRecord> commit = test.diskManager.get_commitMutation();
		long mutationNumber = runner.run((snapshot) -> test.nodeState.mainHandleValidClientMessage(UUID.randomUUID(), ClientMessage.updateConfig(1L, newConfig)));
		Assert.assertEquals(1L, mutationNumber);
		runner.runVoid((snapshot) -> test.nodeState.mainReceivedAckFromDownstream(upstreamEntry1, 1L));
		Assert.assertEquals(1L, commit.get().record.globalOffset);
		
		// Synthesize a call for an election from a peer behind us and verify that this causes us to start an election.
		F<Long> startElection = test.clusterManager.get_mainEnterCandidateState();
		runner.runVoid((snapshot) -> nodeState.mainReceivedRequestForVotes(upstreamEntry1, 2L, 0L, 0L));
		Assert.assertEquals(2L, startElection.get().longValue());
		
		// Stop.
		runner.runVoid((snapshot) -> test.nodeState.mainHandleStopCommand());
		test.join();
	}

	/**
	 * Tests that nodes no longer referenced in any active config are disconnected once the last config referencing them
	 * is committed.
	 * This this, we will interleave config updates into a normal event stream.  The 2 configs will each include 3 nodes
	 * with the receiver and the "leader" being common among them.
	 */
	@Test
	public void testOldDisconnectOldNodes() throws Throwable {
		MainThread test = new MainThread();
		
		// Create our 2 configs.
		ConfigEntry self = test.initialConfig.entries[0];
		ConfigEntry leader = new ConfigEntry(UUID.randomUUID(), new InetSocketAddress(3), new InetSocketAddress(4));
		ConfigEntry peer1 = new ConfigEntry(UUID.randomUUID(), new InetSocketAddress(5), new InetSocketAddress(6));
		ConfigEntry peer2 = new ConfigEntry(UUID.randomUUID(), new InetSocketAddress(7), new InetSocketAddress(8));
		ClusterConfig config1 = ClusterConfig.configFromEntries(new ConfigEntry[] {self, leader, peer1});
		ClusterConfig config2 = ClusterConfig.configFromEntries(new ConfigEntry[] {self, leader, peer2});
		TopicName topic = TopicName.fromString("fake");
		
		// Create the common mutation.
		MutationRecord record1 = MutationRecord.put(1L, 1L, topic, UUID.randomUUID(), 1, new byte[0], new byte[] {1});
		
		// Create the first config change and common mutation.
		MutationRecord record2 = MutationRecord.updateConfig(1L, 2L, UUID.randomUUID(), 1L, config1);
		MutationRecord record3 = MutationRecord.put(1L, 3L, topic, UUID.randomUUID(), 1, new byte[0], new byte[] {2});
		
		// Create the second config change and commont mutation.
		MutationRecord record4 = MutationRecord.updateConfig(1L, 4L, UUID.randomUUID(), 1L, config2);
		MutationRecord record5 = MutationRecord.put(1L, 5L, topic, UUID.randomUUID(), 1, new byte[0], new byte[] {3});
		
		test.start();
		test.startLatch.await();
		Runner runner = new Runner(test.nodeState);
		
		// Send all 5 mutations, verifying that the acks are generated.
		F<Void> becomeFollower = test.clusterManager.get_mainEnterFollowerState();
		F<MutationRecord> received = test.clusterManager.get_mainMutationWasReceivedOrFetched();
		long nextToLoad = runner.run((snapshot) -> test.nodeState.mainAppendMutationFromUpstream(leader, 1L, 0L, record1));
		Assert.assertEquals(2L, nextToLoad);
		becomeFollower.get();
		Assert.assertEquals(record1, received.get());
		
		// Verify that we open 2 new downstream connections with receiving this config.
		F<ConfigEntry> downstreamConnect1 = test.clusterManager.get_mainOpenDownstreamConnection();
		F<ConfigEntry> downstreamConnect2 = test.clusterManager.get_mainOpenDownstreamConnection();
		received = test.clusterManager.get_mainMutationWasReceivedOrFetched();
		nextToLoad = runner.run((snapshot) -> test.nodeState.mainAppendMutationFromUpstream(leader, 1L, 1L, record2));
		Assert.assertEquals(3L, nextToLoad);
		Assert.assertEquals(record2, received.get());
		// (we know these are currently sent back in order so we can check these, directly).
		Assert.assertEquals(leader.nodeUuid, downstreamConnect1.get().nodeUuid);
		Assert.assertEquals(peer1.nodeUuid, downstreamConnect2.get().nodeUuid);
		
		received = test.clusterManager.get_mainMutationWasReceivedOrFetched();
		nextToLoad = runner.run((snapshot) -> test.nodeState.mainAppendMutationFromUpstream(leader, 1L, 1L, record3));
		Assert.assertEquals(4L, nextToLoad);
		Assert.assertEquals(record3, received.get());
		
		// Verify that we open the last downstream connection.
		F<ConfigEntry> downstreamConnect3 = test.clusterManager.get_mainOpenDownstreamConnection();
		received = test.clusterManager.get_mainMutationWasReceivedOrFetched();
		nextToLoad = runner.run((snapshot) -> test.nodeState.mainAppendMutationFromUpstream(leader, 1L, 1L, record4));
		Assert.assertEquals(5L, nextToLoad);
		Assert.assertEquals(record4, received.get());
		Assert.assertEquals(peer2.nodeUuid, downstreamConnect3.get().nodeUuid);
		
		received = test.clusterManager.get_mainMutationWasReceivedOrFetched();
		nextToLoad = runner.run((snapshot) -> test.nodeState.mainAppendMutationFromUpstream(leader, 1L, 1L, record5));
		Assert.assertEquals(6L, nextToLoad);
		Assert.assertEquals(record5, received.get());
		
		// Now send the commit notifications and observe the disconnects as the configs are committed:
		// -note that the commit processing is only done after the disk returns so we need to send those calls, too
		// -in this test, the first config change is purely additive but the second does drop one peer.
		runner.runVoid((snapshot) -> test.nodeState.mainCommittedMutationOffsetFromUpstream(leader, 1L, 1L));
		runner.runVoid((snapshot) -> test.nodeState.mainMutationWasCommitted(CommittedMutationRecord.create(record1, CommitInfo.Effect.VALID)));
		runner.runVoid((snapshot) -> test.nodeState.mainCommittedMutationOffsetFromUpstream(leader, 1L, 2L));
		runner.runVoid((snapshot) -> test.nodeState.mainMutationWasCommitted(CommittedMutationRecord.create(record2, CommitInfo.Effect.VALID)));
		runner.runVoid((snapshot) -> test.nodeState.mainCommittedMutationOffsetFromUpstream(leader, 1L, 3L));
		runner.runVoid((snapshot) -> test.nodeState.mainMutationWasCommitted(CommittedMutationRecord.create(record3, CommitInfo.Effect.VALID)));
		F<ConfigEntry> disconnect = test.clusterManager.get_mainCloseDownstreamConnection();
		runner.runVoid((snapshot) -> test.nodeState.mainCommittedMutationOffsetFromUpstream(leader, 1L, 4L));
		runner.runVoid((snapshot) -> test.nodeState.mainMutationWasCommitted(CommittedMutationRecord.create(record4, CommitInfo.Effect.VALID)));
		Assert.assertEquals(peer1.nodeUuid, disconnect.get().nodeUuid);
		runner.runVoid((snapshot) -> test.nodeState.mainCommittedMutationOffsetFromUpstream(leader, 1L, 5L));
		runner.runVoid((snapshot) -> test.nodeState.mainMutationWasCommitted(CommittedMutationRecord.create(record5, CommitInfo.Effect.VALID)));
		
		// Stop.
		runner.runVoid((snapshot) -> test.nodeState.mainHandleStopCommand());
		test.join();
	}

	/**
	 * Tests that nodes only referenced by a config which did not commit, and which was reverted from in-flight be a new
	 * leader, are disconnected when the config is dropped from in-flight.
	 * For this test, we will create 2 streams of mutations, both TEMP-CONFIG-TEMP, overlapping only in the first TEMP.
	 * The first leader will send the first stream.  The second leader will then send the second stream, causing the
	 * first CONFIG-TEMP to be dropped.
	 */
	@Test
	public void testDisconnectNodesOnConfigRevert() throws Throwable {
		MainThread test = new MainThread();
		
		// Create our 2 configs - overlapping in self and both leaders but each having another peer.
		ConfigEntry self = test.initialConfig.entries[0];
		ConfigEntry leader1 = new ConfigEntry(UUID.randomUUID(), new InetSocketAddress(3), new InetSocketAddress(4));
		ConfigEntry leader2 = new ConfigEntry(UUID.randomUUID(), new InetSocketAddress(5), new InetSocketAddress(6));
		ConfigEntry peer1 = new ConfigEntry(UUID.randomUUID(), new InetSocketAddress(7), new InetSocketAddress(8));
		ConfigEntry peer2 = new ConfigEntry(UUID.randomUUID(), new InetSocketAddress(9), new InetSocketAddress(10));
		ClusterConfig config1 = ClusterConfig.configFromEntries(new ConfigEntry[] {self, leader1, leader2, peer1});
		ClusterConfig config2 = ClusterConfig.configFromEntries(new ConfigEntry[] {self, leader1, leader2, peer2});
		TopicName topic = TopicName.fromString("fake");
		
		// Create the common mutation.
		MutationRecord record1 = MutationRecord.put(1L, 1L, topic, UUID.randomUUID(), 1, new byte[0], new byte[] {1});
		
		// Create the first config change and TEMP mutation.
		MutationRecord record12 = MutationRecord.updateConfig(1L, 2L, UUID.randomUUID(), 1L, config1);
		MutationRecord record13 = MutationRecord.put(1L, 3L, topic, UUID.randomUUID(), 1, new byte[0], new byte[] {2});
		
		// Create the second config change and TEMP mutation.
		MutationRecord record22 = MutationRecord.updateConfig(2L, 2L, UUID.randomUUID(), 1L, config2);
		MutationRecord record23 = MutationRecord.put(2L, 3L, topic, UUID.randomUUID(), 1, new byte[0], new byte[] {3});
		
		test.start();
		test.startLatch.await();
		Runner runner = new Runner(test.nodeState);
		
		// Send the first 3 mutations from stream1, verifying that the acks are generated.
		F<Void> becomeFollower = test.clusterManager.get_mainEnterFollowerState();
		F<MutationRecord> received = test.clusterManager.get_mainMutationWasReceivedOrFetched();
		long nextToLoad = runner.run((snapshot) -> test.nodeState.mainAppendMutationFromUpstream(leader1, 1L, 0L, record1));
		Assert.assertEquals(2L, nextToLoad);
		becomeFollower.get();
		Assert.assertEquals(record1, received.get());
		
		// Verify that we open 3 new downstream connections with receiving this config.
		F<ConfigEntry> downstreamConnect1 = test.clusterManager.get_mainOpenDownstreamConnection();
		F<ConfigEntry> downstreamConnect2 = test.clusterManager.get_mainOpenDownstreamConnection();
		F<ConfigEntry> downstreamConnect3 = test.clusterManager.get_mainOpenDownstreamConnection();
		received = test.clusterManager.get_mainMutationWasReceivedOrFetched();
		nextToLoad = runner.run((snapshot) -> test.nodeState.mainAppendMutationFromUpstream(leader1, 1L, 1L, record12));
		Assert.assertEquals(3L, nextToLoad);
		Assert.assertEquals(record12, received.get());
		// (we know these are currently sent back in order so we can check these, directly).
		Assert.assertEquals(leader1.nodeUuid, downstreamConnect1.get().nodeUuid);
		Assert.assertEquals(leader2.nodeUuid, downstreamConnect2.get().nodeUuid);
		Assert.assertEquals(peer1.nodeUuid, downstreamConnect3.get().nodeUuid);
		
		received = test.clusterManager.get_mainMutationWasReceivedOrFetched();
		nextToLoad = runner.run((snapshot) -> test.nodeState.mainAppendMutationFromUpstream(leader1, 1L, 1L, record13));
		Assert.assertEquals(4L, nextToLoad);
		Assert.assertEquals(record13, received.get());
		
		// We now emulate a new leader taking over to send the second stream, rooted at the common mutation 1.
		// Note that we revert the old mutation before applying the new one so we see 3 disconnects and 3 new connections (even though 2 of them are in both).
		// We should see peer1 disconnect and peer2 connect.
		F<ConfigEntry> laterDownstreamDisconnect1 = test.clusterManager.get_mainCloseDownstreamConnection();
		F<ConfigEntry> laterDownstreamDisconnect2 = test.clusterManager.get_mainCloseDownstreamConnection();
		F<ConfigEntry> laterDownstreamDisconnect3 = test.clusterManager.get_mainCloseDownstreamConnection();
		F<ConfigEntry> laterDownstreamConnect1 = test.clusterManager.get_mainOpenDownstreamConnection();
		F<ConfigEntry> laterDownstreamConnect2 = test.clusterManager.get_mainOpenDownstreamConnection();
		F<ConfigEntry> laterDownstreamConnect3 = test.clusterManager.get_mainOpenDownstreamConnection();
		received = test.clusterManager.get_mainMutationWasReceivedOrFetched();
		nextToLoad = runner.run((snapshot) -> test.nodeState.mainAppendMutationFromUpstream(leader2, 2L, 1L, record22));
		Assert.assertEquals(3L, nextToLoad);
		Assert.assertEquals(record22, received.get());
		// (note that the disconnects don't come in a specific order).
		HashSet<UUID> disconnectIds = new HashSet<>();
		Assert.assertTrue(disconnectIds.add(laterDownstreamDisconnect1.get().nodeUuid));
		Assert.assertTrue(disconnectIds.add(laterDownstreamDisconnect2.get().nodeUuid));
		Assert.assertTrue(disconnectIds.add(laterDownstreamDisconnect3.get().nodeUuid));
		Assert.assertTrue(disconnectIds.contains(leader1.nodeUuid));
		Assert.assertTrue(disconnectIds.contains(leader2.nodeUuid));
		Assert.assertTrue(disconnectIds.contains(peer1.nodeUuid));
		// (but we know connects are found in-order).
		Assert.assertEquals(leader1.nodeUuid, laterDownstreamConnect1.get().nodeUuid);
		Assert.assertEquals(leader2.nodeUuid, laterDownstreamConnect2.get().nodeUuid);
		Assert.assertEquals(peer2.nodeUuid, laterDownstreamConnect3.get().nodeUuid);
		
		received = test.clusterManager.get_mainMutationWasReceivedOrFetched();
		nextToLoad = runner.run((snapshot) -> test.nodeState.mainAppendMutationFromUpstream(leader1, 2L, 2L, record23));
		Assert.assertEquals(4L, nextToLoad);
		Assert.assertEquals(record23, received.get());
		
		// We now send the commits from the new leader and we should observe all 3 commit (note that the leader won't commit 1L, since it didn't send it, but will start at 2L).
		F<CommittedMutationRecord> commit1 = test.diskManager.get_commitMutation();
		F<CommittedMutationRecord> commit2 = test.diskManager.get_commitMutation();
		F<CommittedMutationRecord> commit3 = test.diskManager.get_commitMutation();
		runner.runVoid((snapshot) -> test.nodeState.mainCommittedMutationOffsetFromUpstream(leader2, 2L, record22.globalOffset));
		Assert.assertEquals(record1, commit1.get().record);
		Assert.assertEquals(record22, commit2.get().record);
		runner.runVoid((snapshot) -> test.nodeState.mainMutationWasCommitted(CommittedMutationRecord.create(record1, CommitInfo.Effect.VALID)));
		runner.runVoid((snapshot) -> test.nodeState.mainMutationWasCommitted(CommittedMutationRecord.create(record22, CommitInfo.Effect.VALID)));
		runner.runVoid((snapshot) -> test.nodeState.mainCommittedMutationOffsetFromUpstream(leader2, 2L, record23.globalOffset));
		Assert.assertEquals(record23, commit3.get().record);
		runner.runVoid((snapshot) -> test.nodeState.mainMutationWasCommitted(CommittedMutationRecord.create(record23, CommitInfo.Effect.VALID)));
		
		// Stop.
		runner.runVoid((snapshot) -> test.nodeState.mainHandleStopCommand());
		test.join();
	}

	/**
	 * A node in the CANDIDATE state must become a FOLLOWER if it receives a mutation from a later term.
	 */
	@Test
	public void testCandidateToFollower() throws Throwable {
		MainThread test = new MainThread();
		test.start();
		test.startLatch.await();
		NodeState nodeState = test.nodeState;
		Runner runner = new Runner(nodeState);
		ConfigEntry originalEntry = test.initialConfig.entries[0];
		ConfigEntry upstreamEntry = new ConfigEntry(UUID.randomUUID(), new InetSocketAddress(3), new InetSocketAddress(4));
		ClusterConfig newConfig = ClusterConfig.configFromEntries(new ConfigEntry[] {originalEntry, upstreamEntry});
		TopicName topic = TopicName.fromString("fake");
		
		// Send the node this mutation so it becomes a FOLLOWER and then we can tell it to become a CANDIDATE (can't start election in empty config).
		MutationRecord configChangeRecord = MutationRecord.updateConfig(1L, 1L, UUID.randomUUID(), 1L, newConfig);
		long nextToLoad = runner.run((snapshot) -> nodeState.mainAppendMutationFromUpstream(upstreamEntry, 1L, 0L, configChangeRecord));
		Assert.assertEquals(configChangeRecord.globalOffset + 1, nextToLoad);
		// Tell it the first mutation committed (meaning that config will be active).
		runner.runVoid((snapshot) -> nodeState.mainCommittedMutationOffsetFromUpstream(upstreamEntry, 1L, 1L));
		
		// Force it to enter CANDIDATE state.
		F<Long> electionStart = test.clusterManager.get_mainEnterCandidateState();
		runner.runVoid((snapshot) -> nodeState.mainForceLeader());
		Assert.assertEquals(2L, electionStart.get().longValue());
		
		// This node is now holding an election in term 2 so we will need to send it a heart beat from term 3.
		F<Void> becomeFollower = test.clusterManager.get_mainEnterFollowerState();
		MutationRecord tempRecord = MutationRecord.put(3L, 2L, topic, UUID.randomUUID(), 1, new byte[0], new byte[] {1});
		nextToLoad = runner.run((snapshot) -> nodeState.mainAppendMutationFromUpstream(upstreamEntry, tempRecord.termNumber, configChangeRecord.termNumber, tempRecord));
		becomeFollower.get();
		Assert.assertEquals(tempRecord.globalOffset + 1, nextToLoad);
		
		// Stop.
		runner.runVoid((snapshot) -> test.nodeState.mainHandleStopCommand());
		test.join();
	}


	private static ClusterConfig _createConfig() {
		return ClusterConfig.configFromEntries(new ConfigEntry[] {new ConfigEntry(UUID.randomUUID(), new InetSocketAddress(1), new InetSocketAddress(2))});
	}


	/**
	 * Creates the components and NodeState and then runs them in an internal thread as the "main thread" from the
	 * perspective of Laminar.
	 */
	private static class MainThread extends Thread {
		public final ClusterConfig initialConfig;
		public NodeState nodeState;
		public FutureClientManager clientManager;
		public FutureClusterManager clusterManager;
		public FutureDiskManager diskManager;
		public CountDownLatch startLatch;
		
		public MainThread() {
			this.initialConfig = _createConfig();
			this.startLatch = new CountDownLatch(1);
		}
		
		@Override
		public void run() {
			this.nodeState = new NodeState(this.initialConfig);
			this.clientManager = new FutureClientManager();
			this.clusterManager = new FutureClusterManager();
			this.diskManager = new FutureDiskManager();
			
			this.nodeState.registerClientManager(this.clientManager);
			this.nodeState.registerClusterManager(this.clusterManager);
			this.nodeState.registerConsoleManager(new IConsoleManager() {});
			this.nodeState.registerDiskManager(this.diskManager);
			
			this.startLatch.countDown();
			this.nodeState.runUntilShutdown();
		}
	}
}
