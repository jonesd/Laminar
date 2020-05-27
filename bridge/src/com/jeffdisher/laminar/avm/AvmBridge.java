package com.jeffdisher.laminar.avm;

import java.util.List;
import java.util.UUID;

import org.aion.avm.core.AvmConfiguration;
import org.aion.avm.core.AvmImpl;
import org.aion.avm.core.CommonAvmFactory;
import org.aion.avm.core.ExecutionType;
import org.aion.avm.core.FutureResult;
import org.aion.avm.userlib.CodeAndArguments;
import org.aion.types.Transaction;

import com.jeffdisher.laminar.types.TopicName;
import com.jeffdisher.laminar.types.event.EventRecord;
import com.jeffdisher.laminar.utils.Assert;


public class AvmBridge {
	private final OneCallCapabilities _capabilities;
	private final AvmImpl _avm;
	private final LaminarExternalStateAdapter _kernel;

	public AvmBridge() {
		_capabilities = new OneCallCapabilities();
		AvmConfiguration config = new AvmConfiguration();
		_avm = CommonAvmFactory.buildAvmInstanceForConfiguration(_capabilities, config);
		_kernel = new LaminarExternalStateAdapter();
	}

	public void start() {
		// The AVM factory helper we are using returns a pre-started instance.
	}

	public List<EventRecord> runCreate(long termNumber, long globalOffset, long initialLocalOffset, UUID clientId, long clientNonce, TopicName topicName, byte[] code, byte[] arguments) {
		CodeAndArguments codeAndArgs = new CodeAndArguments(code, arguments);
		Transaction[] transactions = new Transaction[] {
				LaminarAvmTranscoder.createTopic(clientId, clientNonce, topicName, codeAndArgs.encodeToBytes())
		};
		ExecutionType executionType = null;
		long commonMainchainBlockNumber = 1L;
		_capabilities.setCurrentTopic(topicName);
		FutureResult[] results = _avm.run(_kernel, transactions, executionType, commonMainchainBlockNumber);
		Assert.assertTrue(transactions.length == results.length);
		LaminarAvmTranscoder.decodeFuture(results[0]);
		return _kernel.createOutputEventRecords(termNumber, globalOffset, initialLocalOffset, clientId, clientNonce);
	}

	public List<EventRecord> runPut(long termNumber, long globalOffset, long initialLocalOffset, UUID clientId, long clientNonce, TopicName topicName, byte[] key, byte[] value) {
		Transaction[] transactions = new Transaction[] {
				LaminarAvmTranscoder.put(clientId, clientNonce, topicName, key, value)
		};
		ExecutionType executionType = null;
		long commonMainchainBlockNumber = 1L;
		_capabilities.setCurrentTopic(topicName);
		FutureResult[] results = _avm.run(_kernel, transactions, executionType, commonMainchainBlockNumber);
		Assert.assertTrue(transactions.length == results.length);
		LaminarAvmTranscoder.decodeFuture(results[0]);
		return _kernel.createOutputEventRecords(termNumber, globalOffset, initialLocalOffset, clientId, clientNonce);
	}

	public List<EventRecord> runDelete(long termNumber, long globalOffset, long initialLocalOffset, UUID clientId, long clientNonce, TopicName topicName, byte[] key) {
		Transaction[] transactions = new Transaction[] {
				LaminarAvmTranscoder.delete(clientId, clientNonce, topicName, key)
		};
		ExecutionType executionType = null;
		long commonMainchainBlockNumber = 1L;
		_capabilities.setCurrentTopic(topicName);
		FutureResult[] results = _avm.run(_kernel, transactions, executionType, commonMainchainBlockNumber);
		Assert.assertTrue(transactions.length == results.length);
		LaminarAvmTranscoder.decodeFuture(results[0]);
		return _kernel.createOutputEventRecords(termNumber, globalOffset, initialLocalOffset, clientId, clientNonce);
	}

	public void shutdown() {
		_avm.shutdown();
	}
}
