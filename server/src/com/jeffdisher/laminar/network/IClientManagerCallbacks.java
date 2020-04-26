package com.jeffdisher.laminar.network;

import java.util.UUID;
import java.util.function.Consumer;

import com.jeffdisher.laminar.state.StateSnapshot;
import com.jeffdisher.laminar.types.ClientMessage;


public interface IClientManagerCallbacks {
	void ioEnqueueClientCommandForMainThread(Consumer<StateSnapshot> command);

	/**
	 * Called to provide a message which arrived from a normal client.
	 * The nonce management and ACKs are all handled on the caller side, based on the response from this method:  either
	 * being received+committed (after delay) or error.
	 * 
	 * @param clientId The UUID of the client which send the message.
	 * @param incoming The message received.
	 * @return 0 if this the message was an error or a positive global mutation offset of the message if it should be
	 * acked normally (the caller will also delay a commit until notified that this mutation offset is durable).
	 */
	long mainHandleValidClientMessage(UUID clientId, ClientMessage incoming);

	void mainRequestMutationFetch(long mutationOffsetToFetch);

	void mainRequestEventFetch(long nextLocalEventToFetch);
}