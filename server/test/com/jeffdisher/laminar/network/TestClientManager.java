package com.jeffdisher.laminar.network;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.ServerSocketChannel;
import java.util.UUID;
import java.util.function.Consumer;

import org.junit.Assert;
import org.junit.Test;

import com.jeffdisher.laminar.components.NetworkManager;
import com.jeffdisher.laminar.disk.CommittedMutationRecord;
import com.jeffdisher.laminar.state.Helpers;
import com.jeffdisher.laminar.state.StateSnapshot;
import com.jeffdisher.laminar.types.ClusterConfig;
import com.jeffdisher.laminar.types.CommitInfo;
import com.jeffdisher.laminar.types.ConfigEntry;
import com.jeffdisher.laminar.types.TopicName;
import com.jeffdisher.laminar.types.event.EventRecord;
import com.jeffdisher.laminar.types.message.ClientMessage;
import com.jeffdisher.laminar.types.message.ClientMessagePayload_KeyPut;
import com.jeffdisher.laminar.types.mutation.MutationRecord;
import com.jeffdisher.laminar.types.payload.Payload_KeyPut;
import com.jeffdisher.laminar.types.response.ClientResponse;
import com.jeffdisher.laminar.types.response.ClientResponsePayload_ClusterConfig;
import com.jeffdisher.laminar.types.response.ClientResponsePayload_ConfigEntry;
import com.jeffdisher.laminar.types.response.ClientResponseType;
import com.jeffdisher.laminar.utils.TestingHelpers;


public class TestClientManager {
	private static final int PORT_BASE = 3100;

	/**
	 * In this test, we start a ClientManager and emulate a client connecting to it and sending a temp message.
	 * This involves managing the normal HANDSHAKE and CLIENT_READY message flow prior to sending the message.
	 * Finally, we just verify that the ClientManager did observe the expected message.
	 */
	@Test
	public void testReceiveTempMessage() throws Throwable {
		// Create a message.
		ClientMessage message = ClientMessage.put(1L, TopicName.fromString("test"), new byte[0], new byte[] {0,1,2,3});
		// Create a server.
		int port = PORT_BASE + 1;
		ConfigEntry self = new ConfigEntry(UUID.randomUUID(), new InetSocketAddress(9999), new InetSocketAddress(port));
		ServerSocketChannel socket = TestingHelpers.createServerSocket(port);
		LatchedCallbacks callbacks = new LatchedCallbacks();
		ClientManager manager = new ClientManager(self, socket, callbacks);
		manager.startAndWaitForReady();
		
		// Create the connection and send the "temp" message through, directly.
		NetworkManager.NodeToken connectedNode = null;
		try (Socket client = new Socket("localhost", port)) {
			connectedNode = callbacks.runRunnableAndGetNewClientNode(manager);
			Assert.assertNotNull(connectedNode);
			OutputStream toServer = client.getOutputStream();
			InputStream fromServer = client.getInputStream();
			
			// Write our handshake to end up in the "normal client" state.
			TestingHelpers.writeMessageInFrame(toServer, ClientMessage.handshake(UUID.randomUUID()).serialize());
			
			// Run the callbacks once to allow the ClientManager to do the state transition.
			ClientMessage readMessage = callbacks.runAndGetNextMessage();
			Assert.assertNull(readMessage);
			// (we need to run it a second time because of the way the CLIENT_READY is queued)
			readMessage = callbacks.runAndGetNextMessage();
			Assert.assertNull(readMessage);
			
			// Read the CLIENT_READY.
			byte[] raw = TestingHelpers.readMessageInFrame(fromServer);
			ClientResponse ready = ClientResponse.deserialize(raw);
			Assert.assertEquals(ClientResponseType.CLIENT_READY, ready.type);
			Assert.assertEquals(0L, ready.lastCommitGlobalOffset);
			Assert.assertEquals(1L, ready.nonce);
			Assert.assertEquals(1, ((ClientResponsePayload_ClusterConfig)ready.payload).config.entries.length);
			
			// Write the message.
			TestingHelpers.writeMessageInFrame(toServer, message.serialize());
		}
		// We should see the message appear in callbacks.
		ClientMessage output = callbacks.runAndGetNextMessage();
		Assert.assertNotNull(output);
		NetworkManager.NodeToken noNode = callbacks.runRunnableAndGetNewClientNode(manager);
		Assert.assertNull(noNode);
		Assert.assertEquals(message.type, output.type);
		Assert.assertEquals(message.nonce, output.nonce);
		Assert.assertArrayEquals(((ClientMessagePayload_KeyPut)message.payload).key, ((ClientMessagePayload_KeyPut)output.payload).key);
		Assert.assertArrayEquals(((ClientMessagePayload_KeyPut)message.payload).value, ((ClientMessagePayload_KeyPut)output.payload).value);
		
		manager.stopAndWaitForTermination();
		socket.close();
	}

	/**
	 * Does the usual handshake but then sends a message and verifies that the client sees the RECEIVED and, once the
	 * manager is told the commit happened, the COMMITTED.
	 */
	@Test
	public void testSendCommitResponse() throws Throwable {
		// Create a message.
		TopicName topic = TopicName.fromString("test");
		ClientMessage message = ClientMessage.put(1L, topic, new byte[0], new byte[] {0,1,2,3});
		// Create a server.
		int port = PORT_BASE + 2;
		ConfigEntry self = new ConfigEntry(UUID.randomUUID(), new InetSocketAddress(9999), new InetSocketAddress(port));
		ServerSocketChannel socket = TestingHelpers.createServerSocket(port);
		LatchedCallbacks callbacks = new LatchedCallbacks();
		UUID clientId = UUID.randomUUID();
		ClientManager manager = new ClientManager(self, socket, callbacks);
		manager.startAndWaitForReady();
		
		// Create the connection, send the commit message, and read it, directly.
		try (Socket client = new Socket("localhost", port)) {
			NetworkManager.NodeToken connectedNode = callbacks.runRunnableAndGetNewClientNode(manager);
			Assert.assertNotNull(connectedNode);
			OutputStream toServer = client.getOutputStream();
			InputStream fromServer = client.getInputStream();
			
			// Write our handshake to end up in the "normal client" state.
			TestingHelpers.writeMessageInFrame(toServer, ClientMessage.handshake(UUID.randomUUID()).serialize());
			
			// Run the callbacks once to allow the ClientManager to do the state transition.
			ClientMessage readMessage = callbacks.runAndGetNextMessage();
			Assert.assertNull(readMessage);
			// (we need to run it a second time because of the way the CLIENT_READY is queued)
			readMessage = callbacks.runAndGetNextMessage();
			Assert.assertNull(readMessage);
			
			// Read the CLIENT_READY.
			byte[] raw = TestingHelpers.readMessageInFrame(fromServer);
			ClientResponse ready = ClientResponse.deserialize(raw);
			Assert.assertEquals(ClientResponseType.CLIENT_READY, ready.type);
			Assert.assertEquals(0L, ready.lastCommitGlobalOffset);
			Assert.assertEquals(1L, ready.nonce);
			Assert.assertEquals(1, ((ClientResponsePayload_ClusterConfig)ready.payload).config.entries.length);
			
			// Write the message and read the RECEIVED.
			TestingHelpers.writeMessageInFrame(toServer, message.serialize());
			ClientMessage output = callbacks.runAndGetNextMessage();
			Assert.assertNotNull(output);
			raw = TestingHelpers.readMessageInFrame(fromServer);
			ClientResponse received = ClientResponse.deserialize(raw);
			Assert.assertEquals(ClientResponseType.RECEIVED, received.type);
			Assert.assertEquals(0L, received.lastCommitGlobalOffset);
			Assert.assertEquals(1L, received.nonce);
			// (process writable)
			readMessage = callbacks.runAndGetNextMessage();
			Assert.assertNull(readMessage);
			
			// Tell the manager we committed it and verify that we see the commit.
			MutationRecord record = Helpers.convertClientMessageToMutation(message, 1L, clientId, 1L);
			manager.mainProcessingPendingMessageForRecord(CommittedMutationRecord.create(record, CommitInfo.Effect.VALID));
			raw = TestingHelpers.readMessageInFrame(fromServer);
			ClientResponse committed = ClientResponse.deserialize(raw);
			Assert.assertEquals(ClientResponseType.COMMITTED, committed.type);
			Assert.assertEquals(1L, committed.lastCommitGlobalOffset);
			Assert.assertEquals(1L, committed.nonce);
		}
		NetworkManager.NodeToken noNode = callbacks.runRunnableAndGetNewClientNode(manager);
		Assert.assertNull(noNode);
		
		manager.stopAndWaitForTermination();
		socket.close();
	}

	@Test
	public void testSendEvent() throws Throwable {
		// Create an event record.
		TopicName topic = TopicName.fromString("test");
		EventRecord record = EventRecord.put(1L, 1L, 1L, UUID.randomUUID(), 1L, new byte[0], new byte[] { 1, 2, 3});
		// Create a server.
		int port = PORT_BASE + 3;
		ConfigEntry self = new ConfigEntry(UUID.randomUUID(), new InetSocketAddress(9999), new InetSocketAddress(port));
		ServerSocketChannel socket = TestingHelpers.createServerSocket(port);
		LatchedCallbacks callbacks = new LatchedCallbacks();
		ClientManager manager = new ClientManager(self, socket, callbacks);
		manager.startAndWaitForReady();
		
		// Create the connection, send the commit message, and read it, directly.
		try (Socket client = new Socket("localhost", port)) {
			NetworkManager.NodeToken connectedNode = callbacks.runRunnableAndGetNewClientNode(manager);
			Assert.assertNotNull(connectedNode);
			// Write the listen since we want to go into the listener state.
			TestingHelpers.writeMessageInFrame(client.getOutputStream(), ClientMessage.listen(topic, 0L).serialize());
			InputStream fromServer = client.getInputStream();
			
			// Run 1 callback to receive the LISTEN.
			callbacks.runRunnableAndGetNewClientNode(manager);
			// Consume the config it sent in response.
			TestingHelpers.readMessageInFrame(fromServer);
			// Run the next callback so the listener becomes writable.
			callbacks.runRunnableAndGetNewClientNode(manager);
			
			manager.mainSendRecordToListeners(topic, record);
			// Allocate the frame for the full buffer we know we are going to read.
			byte[] serialized = record.serialize();
			byte[] raw = TestingHelpers.readMessageInFrame(fromServer);
			Assert.assertEquals(serialized.length, raw.length);
			// Deserialize the buffer.
			EventRecord deserialized = EventRecord.deserialize(raw);
			Assert.assertEquals(record.globalOffset, deserialized.globalOffset);
			Assert.assertEquals(record.localOffset, deserialized.localOffset);
			Assert.assertEquals(record.clientId, deserialized.clientId);
			Assert.assertArrayEquals(((Payload_KeyPut)record.payload).key, ((Payload_KeyPut)deserialized.payload).key);
			Assert.assertArrayEquals(((Payload_KeyPut)record.payload).value, ((Payload_KeyPut)deserialized.payload).value);
		}
		NetworkManager.NodeToken noNode = callbacks.runRunnableAndGetNewClientNode(manager);
		Assert.assertNull(noNode);
		
		manager.stopAndWaitForTermination();
		socket.close();
	}

	/**
	 * Tests that we get a redirect message, on the client side, when telling the ClientManager to go into a FOLLOWER
	 * state.
	 */
	@Test
	public void testClientRedirectExisting() throws Throwable {
		// Create a server.
		int port = PORT_BASE + 4;
		ConfigEntry self = new ConfigEntry(UUID.randomUUID(), new InetSocketAddress(9999), new InetSocketAddress(port));
		ServerSocketChannel socket = TestingHelpers.createServerSocket(port);
		LatchedCallbacks callbacks = new LatchedCallbacks();
		ClientManager manager = new ClientManager(self, socket, callbacks);
		manager.startAndWaitForReady();
		
		// Create the connection, send the commit message, and read it, directly.
		try (Socket client = new Socket("localhost", port)) {
			// Send the HANDSHAKE and wait for the CLIENT_READY.
			UUID clientId = UUID.randomUUID();
			// -nodeDidConnect
			callbacks.runAndGetNextMessage();
			TestingHelpers.writeMessageInFrame(client.getOutputStream(), ClientMessage.handshake(clientId).serialize());
			// -nodeReadReady
			callbacks.runAndGetNextMessage();
			// -nodeWriteReady
			callbacks.runAndGetNextMessage();
			InputStream fromServer = client.getInputStream();
			byte[] raw = TestingHelpers.readMessageInFrame(fromServer);
			ClientResponse ready = ClientResponse.deserialize(raw);
			Assert.assertEquals(ClientResponseType.CLIENT_READY, ready.type);
			
			// Now, tell the ClientManager to enter the follower state.
			ConfigEntry entry = new ConfigEntry(UUID.randomUUID(), new InetSocketAddress(9999), new InetSocketAddress(port+1));
			manager.mainEnterFollowerState(entry, new StateSnapshot(null, 0L, 0L, 1L));
			// -nodeWriteReady
			callbacks.runAndGetNextMessage();
			raw = TestingHelpers.readMessageInFrame(fromServer);
			ClientResponse redirect = ClientResponse.deserialize(raw);
			Assert.assertEquals(ClientResponseType.REDIRECT, redirect.type);
			Assert.assertEquals(-1L, redirect.nonce);
			Assert.assertEquals(0L, redirect.lastCommitGlobalOffset);
			Assert.assertEquals(entry.nodeUuid, ((ClientResponsePayload_ConfigEntry)redirect.payload).entry.nodeUuid);
		}
		NetworkManager.NodeToken noNode = callbacks.runRunnableAndGetNewClientNode(manager);
		Assert.assertNull(noNode);
		
		manager.stopAndWaitForTermination();
		socket.close();
	}

	/**
	 * Tests that we get a new client gets a redirect immediately after handshake when in FOLLOWER state.
	 */
	@Test
	public void testClientRedirectNew() throws Throwable {
		// Create a server.
		int port = PORT_BASE + 5;
		ConfigEntry self = new ConfigEntry(UUID.randomUUID(), new InetSocketAddress(9999), new InetSocketAddress(port));
		ServerSocketChannel socket = TestingHelpers.createServerSocket(port);
		LatchedCallbacks callbacks = new LatchedCallbacks();
		ClientManager manager = new ClientManager(self, socket, callbacks);
		manager.startAndWaitForReady();
		
		// Now, tell the ClientManager to enter the follower state.
		ConfigEntry entry = new ConfigEntry(UUID.randomUUID(), new InetSocketAddress(9999), new InetSocketAddress(port+1));
		manager.mainEnterFollowerState(entry, new StateSnapshot(null, 0L, 0L, 1L));
		
		// Create the connection, send the commit message, and read it, directly.
		try (Socket client = new Socket("localhost", port)) {
			// Send the HANDSHAKE and wait for the REDIRECT.
			UUID clientId = UUID.randomUUID();
			// -nodeDidConnect
			callbacks.runAndGetNextMessage();
			TestingHelpers.writeMessageInFrame(client.getOutputStream(), ClientMessage.handshake(clientId).serialize());
			// -nodeReadReady
			callbacks.runAndGetNextMessage();
			// -nodeWriteReady
			callbacks.runAndGetNextMessage();
			InputStream fromServer = client.getInputStream();
			byte[] raw = TestingHelpers.readMessageInFrame(fromServer);
			ClientResponse redirect = ClientResponse.deserialize(raw);
			Assert.assertEquals(ClientResponseType.REDIRECT, redirect.type);
			Assert.assertEquals(-1L, redirect.nonce);
			Assert.assertEquals(0L, redirect.lastCommitGlobalOffset);
			Assert.assertEquals(entry.nodeUuid, ((ClientResponsePayload_ConfigEntry)redirect.payload).entry.nodeUuid);
		}
		NetworkManager.NodeToken noNode = callbacks.runRunnableAndGetNewClientNode(manager);
		Assert.assertNull(noNode);
		
		manager.stopAndWaitForTermination();
		socket.close();
	}


	/**
	 * Used for simple cases where the external test only wants to verify that a call was made when expected.
	 */
	private static class LatchedCallbacks implements IClientManagerCallbacks {
		private final ClusterConfig _dummyConfig = ClusterConfig.configFromEntries(new ConfigEntry[] {new ConfigEntry(UUID.randomUUID(), new InetSocketAddress(5), new InetSocketAddress(6))});
		private Consumer<StateSnapshot> _pendingConsumer;
		public ClientMessage recentMessage;
		
		public synchronized NetworkManager.NodeToken runRunnableAndGetNewClientNode(ClientManager managerToRead) throws InterruptedException {
			_lockedRunOnce();
			NetworkManager.NodeToken node = managerToRead.testingGetOneClientNode();
			return node;
		}
		
		public synchronized ClientMessage runAndGetNextMessage() throws InterruptedException {
			_lockedRunOnce();
			ClientMessage message = this.recentMessage;
			this.recentMessage = null;
			return message;
		}

		@Override
		public synchronized void ioEnqueueClientCommandForMainThread(Consumer<StateSnapshot> command) {
			while (null != _pendingConsumer) {
				try {
					this.wait();
				} catch (InterruptedException e) {
					// We don't use interruption in this test - this is just for lock-step connection testing.
					Assert.fail(e.getLocalizedMessage());
				}
			}
			_pendingConsumer = command;
			this.notifyAll();
		}

		@Override
		public long mainHandleValidClientMessage(UUID clientId, ClientMessage incoming) {
			this.recentMessage = incoming;
			return 1L;
		}

		@Override
		public MutationRecord mainClientFetchMutationIfAvailable(long mutationOffset) {
			Assert.fail("Not used in test");
			return null;
		}

		@Override
		public void mainRequestEventFetch(TopicName topic, long nextLocalEventToFetch) {
			Assert.fail("Not used in test");
		}

		@Override
		public void mainForceLeader() {
			Assert.fail("Not used in test");
		}

		private void _lockedRunOnce() throws InterruptedException {
			while (null == _pendingConsumer) {
				this.wait();
			}
			_pendingConsumer.accept(new StateSnapshot(_dummyConfig, 0L, 0L, 1L));
			_pendingConsumer = null;
			this.notifyAll();
		}
	}
}
