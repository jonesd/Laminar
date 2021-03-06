package com.jeffdisher.laminar.types.event;

import java.nio.ByteBuffer;
import java.util.UUID;

import com.jeffdisher.laminar.types.ClusterConfig;
import com.jeffdisher.laminar.types.payload.IPayload;
import com.jeffdisher.laminar.types.payload.Payload_ConfigChange;
import com.jeffdisher.laminar.types.payload.Payload_TopicCreate;
import com.jeffdisher.laminar.types.payload.Payload_KeyDelete;
import com.jeffdisher.laminar.types.payload.Payload_Empty;
import com.jeffdisher.laminar.types.payload.Payload_KeyPut;
import com.jeffdisher.laminar.utils.Assert;


/**
 * See {@link MutationRecord} for a description of the distinction between MutationRecord and EventRecord.
 * 
 * This class represents the logical representation of the event, as well as its physical
 * serialization/deserialization logic.
 */
public class EventRecord {
	public static EventRecord createTopic(long termNumber, long globalOffset, long localOffset, UUID clientId, long clientNonce, byte[] code, byte[] arguments) {
		// The offsets must be positive.
		Assert.assertTrue(termNumber > 0L);
		Assert.assertTrue(globalOffset > 0L);
		Assert.assertTrue(localOffset > 0L);
		Assert.assertTrue(null != clientId);
		Assert.assertTrue(clientNonce >= 0L);
		Assert.assertTrue(null != code);
		Assert.assertTrue(null != arguments);
		return new EventRecord(EventRecordType.TOPIC_CREATE, termNumber, globalOffset, localOffset, clientId, clientNonce, Payload_TopicCreate.create(code, arguments));
	}

	public static EventRecord destroyTopic(long termNumber, long globalOffset, long localOffset, UUID clientId, long clientNonce) {
		// The offsets must be positive.
		Assert.assertTrue(termNumber > 0L);
		Assert.assertTrue(globalOffset > 0L);
		Assert.assertTrue(localOffset > 0L);
		Assert.assertTrue(null != clientId);
		Assert.assertTrue(clientNonce >= 0L);
		return new EventRecord(EventRecordType.TOPIC_DESTROY, termNumber, globalOffset, localOffset, clientId, clientNonce, Payload_Empty.create());
	}

	public static EventRecord put(long termNumber, long globalOffset, long localOffset, UUID clientId, long clientNonce, byte[] key, byte[] value) {
		// The offsets must be positive.
		Assert.assertTrue(termNumber > 0L);
		Assert.assertTrue(globalOffset > 0L);
		Assert.assertTrue(localOffset > 0L);
		Assert.assertTrue(null != clientId);
		Assert.assertTrue(clientNonce >= 0L);
		Assert.assertTrue(null != key);
		Assert.assertTrue(null != value);
		return new EventRecord(EventRecordType.KEY_PUT, termNumber, globalOffset, localOffset, clientId, clientNonce, Payload_KeyPut.create(key, value));
	}

	public static EventRecord delete(long termNumber, long globalOffset, long localOffset, UUID clientId, long clientNonce, byte[] key) {
		// The offsets must be positive.
		Assert.assertTrue(termNumber > 0L);
		Assert.assertTrue(globalOffset > 0L);
		Assert.assertTrue(localOffset > 0L);
		Assert.assertTrue(null != clientId);
		Assert.assertTrue(clientNonce >= 0L);
		Assert.assertTrue(null != key);
		return new EventRecord(EventRecordType.KEY_DELETE, termNumber, globalOffset, localOffset, clientId, clientNonce, Payload_KeyDelete.create(key));
	}

	/**
	 * Creates the special-case CONFIG_CHANGE EventRecord.  Since the listener only interprets EventRecords, and has no
	 * message framing for any other kind of data, we package a change to the cluster config (which is not part of the
	 * data stream, just cluster meta-data) in an EventRecord.
	 * 
	 * @param config The config to encapsulate.
	 * @return A new EventRecord instance for this special-case.
	 */
	public static EventRecord synthesizeRecordForConfig(ClusterConfig config) {
		return new EventRecord(EventRecordType.CONFIG_CHANGE, -1L, -1L, -1L, new UUID(0L, 0L), -1L, Payload_ConfigChange.create(config));
	}

	/**
	 * Deserializes an EventRecord from raw bytes.  Note that there is no difference between common-case and special-
	 * case EventRecord in the serialized form.
	 * 
	 * @param serialized A serialized EventRecord/
	 * @return A new EventRecord instance.
	 */
	public static EventRecord deserialize(byte[] serialized) {
		ByteBuffer wrapper = ByteBuffer.wrap(serialized);
		int ordinal = (int) wrapper.get();
		if (ordinal >= EventRecordType.values().length) {
			throw Assert.unimplemented("Handle corrupt message");
		}
		EventRecordType type = EventRecordType.values()[ordinal];
		long termNumber = wrapper.getLong();
		long globalOffset = wrapper.getLong();
		long localOffset = wrapper.getLong();
		UUID clientId = new UUID(wrapper.getLong(), wrapper.getLong());
		long clientNonce = wrapper.getLong();
		IPayload payload;
		switch (type) {
		case INVALID:
			throw Assert.unimplemented("Handle invalid deserialization");
		case TOPIC_CREATE:
			payload = Payload_TopicCreate.deserialize(wrapper);
			break;
		case TOPIC_DESTROY:
			payload = Payload_Empty.deserialize(wrapper);
			break;
		case KEY_PUT:
			payload = Payload_KeyPut.deserialize(wrapper);
			break;
		case KEY_DELETE:
			payload = Payload_KeyDelete.deserialize(wrapper);
			break;
		case CONFIG_CHANGE:
			payload = Payload_ConfigChange.deserialize(wrapper);
			break;
		default:
			throw Assert.unreachable("Unmatched deserialization type");
		}
		return new EventRecord(type, termNumber, globalOffset, localOffset, clientId, clientNonce, payload);
	}


	public final EventRecordType type;
	public final long termNumber;
	public final long globalOffset;
	public final long localOffset;
	public final UUID clientId;
	public final long clientNonce;
	public final IPayload payload;
	
	private EventRecord(EventRecordType type, long termNumber, long globalOffset, long localOffset, UUID clientId, long clientNonce, IPayload payload) {
		this.type = type;
		this.termNumber = termNumber;
		this.globalOffset = globalOffset;
		this.localOffset = localOffset;
		this.clientId = clientId;
		this.clientNonce = clientNonce;
		this.payload = payload;
	}

	/**
	 * Serializes the receiver into raw bytes.  Note that there is no difference between common-case and special-case
	 * EventRecord in the serialized form.
	 * 
	 * @return The raw bytes of the serialized receiver.
	 */
	public byte[] serialize() {
		byte[] buffer = new byte[Byte.BYTES + Long.BYTES + Long.BYTES + Long.BYTES + (2 * Long.BYTES) + Long.BYTES + this.payload.serializedSize()];
		ByteBuffer wrapper = ByteBuffer.wrap(buffer);
		wrapper
			.put((byte)this.type.ordinal())
			.putLong(this.termNumber)
			.putLong(this.globalOffset)
			.putLong(this.localOffset)
			.putLong(this.clientId.getMostSignificantBits()).putLong(this.clientId.getLeastSignificantBits())
			.putLong(this.clientNonce)
		;
		this.payload.serializeInto(wrapper);
		return buffer;
	}

	@Override
	public String toString() {
		return "Event(type=" + this.type + ", global=" + this.globalOffset + ", local=" + this.localOffset + ")";
	}
}
