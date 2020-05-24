package com.jeffdisher.laminar.avm;

import java.math.BigInteger;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

import org.aion.avm.core.IExternalState;
import org.aion.types.AionAddress;

import com.jeffdisher.laminar.types.event.EventRecord;
import com.jeffdisher.laminar.utils.Assert;


public class LaminarExternalStateAdapter implements IExternalState {
	private byte[] _transformedCode;
	private byte[] _objectGraph;
	private final List<OutputTuple> _outputList = new LinkedList<>();

	public List<EventRecord> createOutputEventRecords(long termNumber, long globalOffset, long initialLocalOffset, UUID clientId, long clientNonce) {
		List<EventRecord> result = new LinkedList<>();
		long nextLocalOffset = initialLocalOffset;
		for (OutputTuple tuple : _outputList) {
			EventRecord record = EventRecord.put(termNumber, globalOffset, nextLocalOffset, clientId, clientNonce, tuple.key, tuple.value);
			nextLocalOffset += 1;
			result.add(record);
		}
		_outputList.clear();
		return result;
	}

	@Override
	public boolean accountBalanceIsAtLeast(AionAddress arg0, BigInteger arg1) {
		return true;
	}

	@Override
	public boolean accountNonceEquals(AionAddress arg0, BigInteger arg1) {
		return true;
	}

	@Override
	public void adjustBalance(AionAddress arg0, BigInteger arg1) {
	}

	@Override
	public void commit() {
		throw Assert.unreachable("Not Supported");
	}

	@Override
	public void commitTo(IExternalState arg0) {
		throw Assert.unreachable("Not Supported");
	}

	@Override
	public void createAccount(AionAddress arg0) {
		throw Assert.unreachable("Not Supported");
	}

	@Override
	public void deleteAccount(AionAddress arg0) {
		throw Assert.unreachable("Not Supported");
	}

	@Override
	public boolean destinationAddressIsSafeForThisVM(AionAddress arg0) {
		throw Assert.unreachable("Not Supported");
	}

	@Override
	public BigInteger getBalance(AionAddress arg0) {
		return new BigInteger("1000000000");
	}

	@Override
	public BigInteger getBlockDifficulty() {
		return BigInteger.ONE;
	}

	@Override
	public long getBlockEnergyLimit() {
		return 10_000_000L;
	}

	@Override
	public byte[] getBlockHashByNumber(long arg0) {
		throw Assert.unreachable("Not Supported");
	}

	@Override
	public long getBlockNumber() {
		return 1L;
	}

	@Override
	public long getBlockTimestamp() {
		return 1L;
	}

	@Override
	public byte[] getCode(AionAddress arg0) {
		return null;
	}

	@Override
	public AionAddress getMinerAddress() {
		return new AionAddress(new byte[AionAddress.LENGTH]);
	}

	@Override
	public BigInteger getNonce(AionAddress arg0) {
		return BigInteger.ZERO;
	}

	@Override
	public byte[] getObjectGraph(AionAddress arg0) {
		return _objectGraph;
	}

	@Override
	public byte[] getStorage(AionAddress arg0, byte[] arg1) {
		return null;
	}

	@Override
	public byte[] getTransformedCode(AionAddress arg0) {
		return _transformedCode;
	}

	@Override
	public boolean hasAccountState(AionAddress arg0) {
		throw Assert.unreachable("Not Supported");
	}

	@Override
	public boolean hasStorage(AionAddress arg0) {
		return false;
	}

	@Override
	public void incrementNonce(AionAddress arg0) {
	}

	@Override
	public boolean isValidEnergyLimitForCreate(long arg0) {
		return true;
	}

	@Override
	public boolean isValidEnergyLimitForNonCreate(long arg0) {
		return true;
	}

	@Override
	public IExternalState newChildExternalState() {
		throw Assert.unreachable("Not Supported");
	}

	@Override
	public void putCode(AionAddress arg0, byte[] arg1) {
	}

	@Override
	public void putObjectGraph(AionAddress arg0, byte[] arg1) {
		_objectGraph = arg1;
	}

	@Override
	public void putStorage(AionAddress arg0, byte[] arg1, byte[] arg2) {
		// We actually use this to synthesize EventRecord outputs from the execution (until changes are made to the embedded AVM API to make this a natural thing).
		_outputList.add(new OutputTuple(arg1, arg2));
	}

	@Override
	public void refundAccount(AionAddress arg0, BigInteger arg1) {
		throw Assert.unreachable("Not Supported");
	}

	@Override
	public void removeStorage(AionAddress arg0, byte[] arg1) {
		throw Assert.unreachable("Not Supported");
	}

	@Override
	public void setTransformedCode(AionAddress arg0, byte[] arg1) {
		Assert.assertTrue(null == _transformedCode);
		_transformedCode = arg1;
	}


	private static class OutputTuple {
		public final byte[] key;
		public final byte[] value;
		public OutputTuple(byte[] key, byte[] value) {
			this.key = key.clone();
			this.value = value.clone();
		}
	}
}
