package com.jeffdisher.laminar.network.p2p;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;

import com.jeffdisher.laminar.network.p2p.DownstreamMessage;
import com.jeffdisher.laminar.network.p2p.DownstreamPayload_Identity;
import com.jeffdisher.laminar.types.ClusterConfig;
import com.jeffdisher.laminar.types.ConfigEntry;
import com.jeffdisher.laminar.types.TopicName;
import com.jeffdisher.laminar.types.mutation.MutationRecord;
import com.jeffdisher.laminar.types.payload.Payload_KeyPut;


public class TestDownstreamMessage {
	@Test
	public void testIdentity() throws Throwable {
		InetAddress localhost = InetAddress.getLocalHost();
		InetSocketAddress cluster = ClusterConfig.cleanSocketAddress(new InetSocketAddress(localhost, 1000));
		InetSocketAddress client = ClusterConfig.cleanSocketAddress(new InetSocketAddress(localhost, 1001));
		ConfigEntry entry = new ConfigEntry(UUID.randomUUID(), cluster, client);
		DownstreamMessage message = DownstreamMessage.identity(entry);
		int size = message.serializedSize();
		ByteBuffer buffer = ByteBuffer.allocate(size);
		message.serializeInto(buffer);
		buffer.flip();
		
		DownstreamMessage test = DownstreamMessage.deserializeFrom(buffer);
		DownstreamPayload_Identity payload = (DownstreamPayload_Identity)test.payload;
		Assert.assertEquals(entry.nodeUuid, payload.self.nodeUuid);
	}

	@Test
	public void testAppendMutations() throws Throwable {
		TopicName topic = TopicName.fromString("test");
		MutationRecord mutation = MutationRecord.put(1L, 1L, topic, UUID.randomUUID(), 1L, new byte[0], new byte[] {1,2,3});
		long lastCommittedMutationOffset = 1L;
		DownstreamMessage message = DownstreamMessage.appendMutations(1L, 0L, mutation, lastCommittedMutationOffset);
		int size = message.serializedSize();
		ByteBuffer buffer = ByteBuffer.allocate(size);
		message.serializeInto(buffer);
		buffer.flip();
		
		DownstreamMessage test = DownstreamMessage.deserializeFrom(buffer);
		DownstreamPayload_AppendMutations payload = (DownstreamPayload_AppendMutations)test.payload;
		Assert.assertEquals(1L, payload.termNumber);
		Assert.assertEquals(lastCommittedMutationOffset, payload.lastCommittedMutationOffset);
		Assert.assertArrayEquals(((Payload_KeyPut)mutation.payload).key, ((Payload_KeyPut)payload.records[0].payload).key);
		Assert.assertArrayEquals(((Payload_KeyPut)mutation.payload).value, ((Payload_KeyPut)payload.records[0].payload).value);
	}

	@Test
	public void testHeartbeat() throws Throwable {
		long lastCommittedMutationOffset = 1L;
		DownstreamMessage message = DownstreamMessage.heartbeat(1L, lastCommittedMutationOffset);
		int size = message.serializedSize();
		ByteBuffer buffer = ByteBuffer.allocate(size);
		message.serializeInto(buffer);
		buffer.flip();
		
		DownstreamMessage test = DownstreamMessage.deserializeFrom(buffer);
		DownstreamPayload_AppendMutations payload = (DownstreamPayload_AppendMutations)test.payload;
		Assert.assertEquals(1L, payload.termNumber);
		Assert.assertEquals(lastCommittedMutationOffset, payload.lastCommittedMutationOffset);
		Assert.assertEquals(0, payload.records.length);
	}

	@Test
	public void testRequestVotes() throws Throwable {
		long newTermNumber = 2L;
		long previousMutationTerm = 1L;
		long previousMuationOffset = 3L;
		DownstreamMessage message = DownstreamMessage.requestVotes(newTermNumber, previousMutationTerm, previousMuationOffset);
		int size = message.serializedSize();
		ByteBuffer buffer = ByteBuffer.allocate(size);
		message.serializeInto(buffer);
		buffer.flip();
		
		DownstreamMessage test = DownstreamMessage.deserializeFrom(buffer);
		DownstreamPayload_RequestVotes payload = (DownstreamPayload_RequestVotes)test.payload;
		Assert.assertEquals(newTermNumber, payload.newTermNumber);
		Assert.assertEquals(previousMutationTerm, payload.previousMutationTerm);
		Assert.assertEquals(previousMuationOffset, payload.previousMuationOffset);
	}
}
