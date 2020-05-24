package com.jeffdisher.laminar.avm;

import java.math.BigInteger;

import org.aion.avm.core.IExternalCapabilities;
import org.aion.types.AionAddress;
import org.aion.types.InternalTransaction;

import com.jeffdisher.laminar.utils.Assert;


/**
 * Laminar is not a blockchain so none of the hashing or meta-transaction support is implemented.
 * The only thing we support is the creation of new "contract" addresses but we expose those as just a serialized
 * version of the topic name.
 * Currently, we squirrel this information away in the transaction nonce, but this is only temporary.
 * TODO:  Update this once we change this interface in the AVM to allow extra context to be passed around since nonce
 * should actually be the nonce, not a hidden piece of data.
 */
public class TopicInNonceCapabilities implements IExternalCapabilities {
	@Override
	public boolean verifyEdDSA(byte[] arg0, byte[] arg1, byte[] arg2) {
		throw Assert.unreachable("Crypto not supported");
	}
	@Override
	public byte[] sha256(byte[] arg0) {
		throw Assert.unreachable("Crypto not supported");
	}
	@Override
	public byte[] keccak256(byte[] arg0) {
		throw Assert.unreachable("Crypto not supported");
	}
	@Override
	public AionAddress generateContractAddress(AionAddress deployerAddress, BigInteger nonce) {
		byte[] topicName = nonce.toByteArray();
		return LaminarAvmTranscoder.addressFromTopicName(topicName);
	}
	@Override
	public InternalTransaction decodeSerializedTransaction(byte[] transactionPayload, AionAddress executor, long energyPrice, long energyLimit) {
		throw Assert.unreachable("Meta-transactions not supported");
	}
	@Override
	public byte[] blake2b(byte[] arg0) {
		throw Assert.unreachable("Crypto not supported");
	}
}
