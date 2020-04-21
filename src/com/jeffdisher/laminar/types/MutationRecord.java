package com.jeffdisher.laminar.types;

import java.nio.ByteBuffer;
import java.util.UUID;

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
	public static MutationRecord generateRecord(MutationRecordType type, long globalOffset, UUID clientId, long clientNonce, byte[] payload) {
		Assert.assertTrue(MutationRecordType.INVALID != type);
		// The offsets must be positive.
		Assert.assertTrue(globalOffset > 0L);
		Assert.assertTrue(null != clientId);
		Assert.assertTrue(clientNonce >= 0L);
		return new MutationRecord(type, globalOffset, clientId, clientNonce, payload);
	}

	public static MutationRecord deserialize(byte[] serialized) {
		ByteBuffer wrapper = ByteBuffer.wrap(serialized);
		int ordinal = (int) wrapper.get();
		if (ordinal >= MutationRecordType.values().length) {
			throw Assert.unimplemented("Handle corrupt message");
		}
		MutationRecordType type = MutationRecordType.values()[ordinal];
		long globalOffset = wrapper.getLong();
		UUID clientId = new UUID(wrapper.getLong(), wrapper.getLong());
		long clientNonce = wrapper.getLong();
		byte[] payload = new byte[wrapper.remaining()];
		wrapper.get(payload);
		return new MutationRecord(type, globalOffset, clientId, clientNonce, payload);
	}


	public final MutationRecordType type;
	public final long globalOffset;
	public final UUID clientId;
	public final long clientNonce;
	public final byte[] payload;
	
	private MutationRecord(MutationRecordType type, long globalOffset, UUID clientId, long clientNonce, byte[] payload) {
		this.type = type;
		this.globalOffset = globalOffset;
		this.clientId = clientId;
		this.clientNonce = clientNonce;
		this.payload = payload;
	}

	public byte[] serialize() {
		byte[] buffer = new byte[Byte.BYTES + Long.BYTES + (2 * Long.BYTES) + Long.BYTES + this.payload.length];
		ByteBuffer wrapper = ByteBuffer.wrap(buffer);
		wrapper
			.put((byte)this.type.ordinal())
			.putLong(this.globalOffset)
			.putLong(this.clientId.getMostSignificantBits()).putLong(this.clientId.getLeastSignificantBits())
			.putLong(this.clientNonce)
			.put(this.payload)
		;
		return buffer;
	}
}
