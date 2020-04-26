package com.jeffdisher.laminar.client;

import com.jeffdisher.laminar.types.ClientMessage;


/**
 * A ClientResult is used by client-side application code to track the progress of a message it sent as it progresses
 * through the cluster.
 * Specifically, the client can block on the cluster receiving the message and/or committing the message.
 * Note that a message being RECEIVED only means it has reached the current leader of the cluster.  It is possible that
 * it will still be re-sent if the cluster fails over to a node which hasn't yet seen the message.
 * A message being COMMITTED means that either DID or absolutely WILL commit in the order received, across the entire
 * cluster.
 */
public class ClientResult {
	public final ClientMessage message;
	private boolean _received;
	private boolean _committed;

	public ClientResult(ClientMessage message) {
		this.message = message;
	}

	/**
	 * Blocks the caller until the message has been received by the current cluster leader.
	 * Note that this isn't generally useful except for verifying that the leader, and network to it, are working
	 * properly.
	 * 
	 * @throws InterruptedException If the user code interrupted this thread.
	 */
	public synchronized void waitForReceived() throws InterruptedException {
		while (!_received) {
			// We allow the user to interrupt their own thread.
			this.wait();
		}
	}

	/**
	 * Blocks the caller until the message has been committed across a majority of the cluster.
	 * This method returning means that the cluster has (or unavoidably will) reached consensus that this message is
	 * part of the cluster's mutation history in the order it was received.
	 * This is the primary means of blocking on "completion" of the message.
	 * 
	 * @throws InterruptedException If the user code interrupted this thread.
	 */
	public synchronized void waitForCommitted() throws InterruptedException {
		while (!_committed) {
			// We allow the user to interrupt their own thread.
			this.wait();
		}
	}

	/**
	 * Called by the lower levels of the ClientConnection to notify that this message has been received.
	 * Note that this state can be reverted if the cluster fails over to a new leader who hasn't yet seen it.
	 */
	public synchronized void setReceived() {
		_received = true;
		this.notifyAll();
	}

	/**
	 * Called by the lower levels of the ClientConnection to notify that this message has been received on a majority of
	 * the nodes in the cluster and will unavoidably be committed.
	 */
	public synchronized void setCommitted() {
		_committed = true;
		this.notifyAll();
	}

	/**
	 * Called by the lower levels of the ClientConnection during a reconnection to the cluster to reset it when it isn't
	 * yet known if the leader has actually received it.
	 */
	public void clearReceived() {
		_received = false;
	}

	/**
	 * Called by the lower levels of ClientConnection at the end of reconnect to determine if this message must be
	 * re-sent.
	 * 
	 * @return True if this message has been marked received.
	 */
	public boolean isReceived() {
		return _received;
	}
}
