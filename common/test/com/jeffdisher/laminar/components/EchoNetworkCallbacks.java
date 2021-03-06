package com.jeffdisher.laminar.components;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

import org.junit.Assert;


/**
 * An implementation of INetworkManagerBackgroundCallbacks intended for use with tests.
 * This implementation uses an internal thread to the message it received back to the client who sent it.
 * There is, however, a decision around what to do with the message, first:  it will be extended by one byte and sent
 * unless it is longer than a stated maximum size, where it will be dropped, instead.
 * This one instance should be able to act as the central server to an arbitrarily large number of test nodes.
 * The internal structures are mostly just handled on the internal thread, where a queue of Runnables is sent from the
 * callback thread into this internal thread to request state changes.
 */
public class EchoNetworkCallbacks implements INetworkManagerBackgroundCallbacks {
	private NetworkManager _manager;
	private final int _maxLength;
	private final CountDownLatch _messageDroppedLatch;
	private final Thread _thread;
	private final BlockingQueue<Runnable> _actions;
	private final Map<NetworkManager.NodeToken, List<byte[]>> _outputBuffers;
	private final Set<NetworkManager.NodeToken> _writeReady;
	private boolean _keepRunning;
	
	public EchoNetworkCallbacks(int maxLength, CountDownLatch messageDroppedLatch) {
		_maxLength = maxLength;
		_messageDroppedLatch = messageDroppedLatch;
		_thread = new Thread() {
			@Override
			public void run() {
				try {
					_threadMain();
				} catch (InterruptedException e) {
					// We don't use interruption.
					Assert.fail();
				}
			}
		};
		_actions = new LinkedBlockingQueue<>();
		_outputBuffers = new HashMap<>();
		_writeReady = new HashSet<>();
	}

	public void startThreadForManager(NetworkManager manager) {
		_manager = manager;
		_thread.start();
	}

	public void stopAndWait() {
		_actions.add(new Runnable() {
			@Override
			public void run() {
				_keepRunning = false;
			}});
		try {
			_thread.join();
		} catch (InterruptedException e) {
			// We don't use interruption.
			Assert.fail();
		}
	}

	@Override
	public void nodeDidConnect(NetworkManager.NodeToken node) {
		// The only interest we have in connections is to mark them write-ready and set up the map entry.
		_setupNewNode(node);
	}

	@Override
	public void nodeDidDisconnect(NetworkManager.NodeToken node, IOException cause) {
		// This doesn't currently do anything with disconnects.
	}

	@Override
	public void nodeWriteReady(NetworkManager.NodeToken node) {
		_actions.add(new Runnable() {
			@Override
			public void run() {
				_writeReady.add(node);
			}});
	}

	@Override
	public void nodeReadReady(NetworkManager.NodeToken node) {
		// Note that this testing implementation applies no reading back-pressure of its own.
		_actions.add(new Runnable() {
			@Override
			public void run() {
				byte[] buffer = _manager.readWaitingMessage(node);
				if (buffer.length < _maxLength) {
					byte[] newBuffer = new byte[buffer.length + 1];
					System.arraycopy(buffer, 0, newBuffer, 0, buffer.length);
					_outputBuffers.get(node).add(newBuffer);
				} else {
					_messageDroppedLatch.countDown();
				}
			}});
	}

	@Override
	public void outboundNodeConnected(NetworkManager.NodeToken node) {
		// We can also handle out-bound connections so do the same setup as for in-bound.
		_setupNewNode(node);
	}

	@Override
	public void outboundNodeDisconnected(NetworkManager.NodeToken node, IOException cause) {
		// This doesn't currently do anything with disconnects.
	}

	@Override
	public void outboundNodeConnectionFailed(NetworkManager.NodeToken token, IOException cause) {
		// This doesn't currently do anything with connection failures.
	}


	private void _setupNewNode(NetworkManager.NodeToken node) {
		_actions.add(new Runnable() {
			@Override
			public void run() {
				_writeReady.add(node);
				_outputBuffers.put(node, new LinkedList<>());
			}});
	}

	private void _threadMain() throws InterruptedException {
		// To make it clear that we only read/write this flag on the internal thread, set it here.
		_keepRunning = true;
		while (_keepRunning) {
			// Block for the action.
			Runnable action = _actions.take();
			// Apply the action (all state is modified only on the internal thread).
			action.run();
			// Check if we need to do anything.
			// (reads are all done in the Runnable but writes are done here).
			for (NetworkManager.NodeToken target : _writeReady) {
				List<byte[]> outgoing = _outputBuffers.get(target);
				if (!outgoing.isEmpty()) {
					// We will only write the first one but may extend this, later, to be more aggressive once tests can
					// drive that much data.
					byte[] buffer = outgoing.remove(0);
					boolean didWrite = _manager.trySendMessage(target, buffer);
					// (this one must pass since we know the buffer is writable)
					Assert.assertTrue(didWrite);
				}
			}
		}
	}
}
