package com.jeffdisher.laminar.types.mutation;

import java.nio.ByteBuffer;
import java.util.UUID;

import com.jeffdisher.laminar.types.ClusterConfig;
import com.jeffdisher.laminar.types.TopicName;
import com.jeffdisher.laminar.utils.Assert;


/**
 * Laminar is built on 2 kinds of data:
 * <ul>
 * <li>Mutations:  These are generated on the leader from valid ClientMessages.  They are written to the "input" stream
 *  when committing to disk.  They are sent to followers in the cluster.  They are sent to reconnecting clients.  In a
 *  sense, they are the "independent variables".</li>
 * <li>Events:  These are generated by every node in the cluster when processing a mutation (when a node decides or is told
 *  that it should commit).  They are written to the "per-topic" stream when committing to disk.  They are only ever
 *  sent to listeners.  In a sense, they are the "dependent variables".  Note that, even though every node creates
 *  these, the mapping from the mutation to event is deterministic so every node derives the same answer.</li>
 * </ul>
 * It is worth noting that that processing a single mutation normally produces a single event but programmable topics
 * can work differently where a mutation can result in 0 or several events being generated.
 * 
 * This class represents the logical representation of the mutation, as well as its physical
 * serialization/deserialization logic.
 */
public class MutationRecord {
	public static MutationRecord createTopic(long termNumber, long globalOffset, TopicName topic, UUID clientId, long clientNonce) {
		// The offsets must be positive.
		Assert.assertTrue(termNumber > 0L);
		Assert.assertTrue(globalOffset > 0L);
		Assert.assertTrue(null != topic);
		Assert.assertTrue(null != clientId);
		Assert.assertTrue(clientNonce >= 0L);
		return new MutationRecord(MutationRecordType.CREATE_TOPIC, termNumber, globalOffset, topic, clientId, clientNonce, MutationRecordPayload_Empty.create());
	}

	public static MutationRecord destroyTopic(long termNumber, long globalOffset, TopicName topic, UUID clientId, long clientNonce) {
		// The offsets must be positive.
		Assert.assertTrue(termNumber > 0L);
		Assert.assertTrue(globalOffset > 0L);
		Assert.assertTrue(null != topic);
		Assert.assertTrue(null != clientId);
		Assert.assertTrue(clientNonce >= 0L);
		return new MutationRecord(MutationRecordType.DESTROY_TOPIC, termNumber, globalOffset, topic, clientId, clientNonce, MutationRecordPayload_Empty.create());
	}

	public static MutationRecord put(long termNumber, long globalOffset, TopicName topic, UUID clientId, long clientNonce, byte[] key, byte[] value) {
		// The offsets must be positive.
		Assert.assertTrue(termNumber > 0L);
		Assert.assertTrue(globalOffset > 0L);
		Assert.assertTrue(null != topic);
		Assert.assertTrue(null != clientId);
		Assert.assertTrue(clientNonce >= 0L);
		Assert.assertTrue(null != key);
		Assert.assertTrue(null != value);
		return new MutationRecord(MutationRecordType.PUT, termNumber, globalOffset, topic, clientId, clientNonce, MutationRecordPayload_Put.create(key, value));
	}

	public static MutationRecord updateConfig(long termNumber, long globalOffset, UUID clientId, long clientNonce, ClusterConfig config) {
		// The offsets must be positive.
		Assert.assertTrue(termNumber > 0L);
		Assert.assertTrue(globalOffset > 0L);
		Assert.assertTrue(null != clientId);
		Assert.assertTrue(clientNonce >= 0L);
		Assert.assertTrue(null != config);
		// UPDATE_CONFIG is not posted to a topic.
		TopicName topic = TopicName.syntheticTopic();
		return new MutationRecord(MutationRecordType.UPDATE_CONFIG, termNumber, globalOffset, topic, clientId, clientNonce, MutationRecordPayload_Config.create(config));
	}

	public static MutationRecord deserialize(byte[] serialized) {
		ByteBuffer wrapper = ByteBuffer.wrap(serialized);
		return _deserializeFrom(wrapper);
	}

	public static MutationRecord deserializeFrom(ByteBuffer buffer) {
		return _deserializeFrom(buffer);
	}


	private static MutationRecord _deserializeFrom(ByteBuffer buffer) {
		int ordinal = (int) buffer.get();
		if (ordinal >= MutationRecordType.values().length) {
			throw Assert.unimplemented("Handle corrupt message");
		}
		MutationRecordType type = MutationRecordType.values()[ordinal];
		long termNumber = buffer.getLong();
		long globalOffset = buffer.getLong();
		TopicName topic = TopicName.deserializeFrom(buffer);
		UUID clientId = new UUID(buffer.getLong(), buffer.getLong());
		long clientNonce = buffer.getLong();
		IMutationRecordPayload payload;
		switch (type) {
		case INVALID:
			throw Assert.unimplemented("Handle invalid deserialization");
		case CREATE_TOPIC:
			payload = MutationRecordPayload_Empty.deserialize(buffer);
			break;
		case DESTROY_TOPIC:
			payload = MutationRecordPayload_Empty.deserialize(buffer);
			break;
		case PUT:
			payload = MutationRecordPayload_Put.deserialize(buffer);
			break;
		case UPDATE_CONFIG:
			payload = MutationRecordPayload_Config.deserialize(buffer);
			break;
		default:
			throw Assert.unreachable("Unmatched deserialization type");
		}
		return new MutationRecord(type, termNumber, globalOffset, topic, clientId, clientNonce, payload);
	}


	public final MutationRecordType type;
	public final long termNumber;
	public final long globalOffset;
	public final TopicName topic;
	public final UUID clientId;
	public final long clientNonce;
	public final IMutationRecordPayload payload;
	
	private MutationRecord(MutationRecordType type, long termNumber, long globalOffset, TopicName topic, UUID clientId, long clientNonce, IMutationRecordPayload payload) {
		this.type = type;
		this.termNumber = termNumber;
		this.globalOffset = globalOffset;
		this.topic = topic;
		this.clientId = clientId;
		this.clientNonce = clientNonce;
		this.payload = payload;
	}

	public byte[] serialize() {
		byte[] buffer = new byte[_serializedSize()];
		ByteBuffer wrapper = ByteBuffer.wrap(buffer);
		_serializeInto(wrapper);
		return buffer;
	}

	public int serializedSize() {
		return _serializedSize();
	}

	public void serializeInto(ByteBuffer buffer) {
		_serializeInto(buffer);
	}


	private int _serializedSize() {
		return Byte.BYTES
				+ Long.BYTES
				+ Long.BYTES
				+ this.topic.serializedSize()
				+ (2 * Long.BYTES)
				+ Long.BYTES
				+ this.payload.serializedSize()
		;
	}

	private void _serializeInto(ByteBuffer buffer) {
		buffer
			.put((byte)this.type.ordinal())
			.putLong(this.termNumber)
			.putLong(this.globalOffset)
		;
		this.topic.serializeInto(buffer);
		buffer
			.putLong(this.clientId.getMostSignificantBits()).putLong(this.clientId.getLeastSignificantBits())
			.putLong(this.clientNonce)
		;
		this.payload.serializeInto(buffer);
	}
}
