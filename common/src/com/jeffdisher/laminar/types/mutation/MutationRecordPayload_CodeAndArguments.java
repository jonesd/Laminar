package com.jeffdisher.laminar.types.mutation;

import java.nio.ByteBuffer;

import com.jeffdisher.laminar.utils.MiscHelpers;


/**
 * For programmable topic creation - contains the serialized code and arguments to deploy in the topic.
 */
public class MutationRecordPayload_CodeAndArguments implements IMutationRecordPayload {
	public static MutationRecordPayload_CodeAndArguments create(byte[] code, byte[] arguments) {
		return new MutationRecordPayload_CodeAndArguments(code, arguments);
	}

	public static MutationRecordPayload_CodeAndArguments deserialize(ByteBuffer serialized) {
		byte[] code = MiscHelpers.readSizedBytes(serialized);
		byte[] arguments = MiscHelpers.readSizedBytes(serialized);
		return new MutationRecordPayload_CodeAndArguments(code, arguments);
	}


	public final byte[] code;
	public final byte[] arguments;

	private MutationRecordPayload_CodeAndArguments(byte[] code, byte[] arguments) {
		this.code = code;
		this.arguments = arguments;
	}

	@Override
	public int serializedSize() {
		return Short.BYTES
				+ this.code.length
				+ Short.BYTES
				+ this.arguments.length
		;
	}

	@Override
	public void serializeInto(ByteBuffer buffer) {
		MiscHelpers.writeSizedBytes(buffer, this.code);
		MiscHelpers.writeSizedBytes(buffer, this.arguments);
	}
}
