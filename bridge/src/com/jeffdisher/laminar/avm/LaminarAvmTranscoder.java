package com.jeffdisher.laminar.avm;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.UUID;

import org.aion.avm.core.FutureResult;
import org.aion.types.AionAddress;
import org.aion.types.Transaction;

import com.jeffdisher.laminar.types.TopicName;
import com.jeffdisher.laminar.utils.Assert;


public class LaminarAvmTranscoder {
	private static BigInteger VALUE = BigInteger.ZERO;
	private static long ENERGY_LIMIT = 1_000_000L;
	private static long ENERGY_PRICE = 1L;

	public static Transaction createTopic(UUID clientId, TopicName topicName, byte[] codeAndArgs) {
		AionAddress sender = _addressFromUuid(clientId);
		byte[] transactionHash = new byte[0];
		// Temporarily, we hide the topic name in the nonce.
		BigInteger senderNonce = new BigInteger(topicName.copyRawBytes());
		return Transaction.contractCreateTransaction(sender, transactionHash, senderNonce, VALUE, codeAndArgs, ENERGY_LIMIT, ENERGY_PRICE);
	}

	public static Transaction put(UUID clientId, TopicName topicName, byte[] key, byte[] value) {
		AionAddress sender = _addressFromUuid(clientId);
		AionAddress destination = _addressFromTopicName(topicName.copyRawBytes());
		byte[] transactionHash = new byte[0];
		// Temporarily, we hide the topic name in the nonce.
		BigInteger senderNonce = new BigInteger(topicName.copyRawBytes());
		// Temporarily, we only provide key.
		return Transaction.contractCallTransaction(sender, destination, transactionHash, senderNonce, VALUE, key, ENERGY_LIMIT, ENERGY_PRICE);
	}

	public static Transaction delete(UUID clientId, TopicName topicName, byte[] key) {
		AionAddress sender = _addressFromUuid(clientId);
		AionAddress destination = _addressFromTopicName(topicName.copyRawBytes());
		byte[] transactionHash = new byte[0];
		// Temporarily, we hide the topic name in the nonce.
		BigInteger senderNonce = new BigInteger(topicName.copyRawBytes());
		// Temporarily, we only provide key.
		return Transaction.contractCallTransaction(sender, destination, transactionHash, senderNonce, VALUE, key, ENERGY_LIMIT, ENERGY_PRICE);
	}

	public static void decodeFuture(FutureResult result) {
		Assert.assertTrue(result.getResult().transactionStatus.isSuccess());
	}

	public static AionAddress addressFromTopicName(byte[] topicName) {
		return _addressFromTopicName(topicName);
	}

	private static AionAddress _addressFromUuid(UUID clientId) {
		long most = clientId.getMostSignificantBits();
		long least = clientId.getLeastSignificantBits();
		ByteBuffer buffer = ByteBuffer.allocate(AionAddress.LENGTH);
		buffer.position(AionAddress.LENGTH - Long.BYTES - Long.BYTES);
		byte[] raw = buffer
				.putLong(most)
				.putLong(least)
				.array();
		return new AionAddress(raw);
	}

	private static AionAddress _addressFromTopicName(byte[] topicName) {
		Assert.assertTrue(topicName.length <= AionAddress.LENGTH);
		byte[] addressContainer = new byte[AionAddress.LENGTH];
		System.arraycopy(topicName, 0, addressContainer, 0, topicName.length);
		return new AionAddress(addressContainer);
	}
}
