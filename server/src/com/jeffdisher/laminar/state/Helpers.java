package com.jeffdisher.laminar.state;

import java.util.UUID;

import com.jeffdisher.laminar.types.ClusterConfig;
import com.jeffdisher.laminar.types.message.ClientMessage;
import com.jeffdisher.laminar.types.message.ClientMessagePayload_TopicCreate;
import com.jeffdisher.laminar.types.message.ClientMessagePayload_KeyDelete;
import com.jeffdisher.laminar.types.message.ClientMessagePayload_KeyPut;
import com.jeffdisher.laminar.types.message.ClientMessagePayload_TopicDestroy;
import com.jeffdisher.laminar.types.message.ClientMessagePayload_ConfigChange;
import com.jeffdisher.laminar.types.mutation.MutationRecord;
import com.jeffdisher.laminar.utils.Assert;


/**
 * Miscellaneous helper routines used by NodeState but easier to test and maintain if kept detached from it.
 * These may move elsewhere or be renamed, in the future.
 */
public class Helpers {
	/**
	 * Converts the given ClientMessage from the given clientId into a MutationRecord, assigned the given
	 * mutationOffsetToAssign.
	 * 
	 * @param message The message from the client.
	 * @param termNumber The current term number of the cluster.
	 * @param clientId The client who sent the message.
	 * @param mutationOffsetToAssign The global mutation offset to assign to the created mutation.
	 * @return The MutationRecord for this ClientMessage.
	 */
	public static MutationRecord convertClientMessageToMutation(ClientMessage message, long termNumber, UUID clientId, long mutationOffsetToAssign) {
		MutationRecord converted = null;
		switch (message.type) {
		case INVALID:
			Assert.unimplemented("Invalid message type");
			break;
		case TOPIC_CREATE: {
			ClientMessagePayload_TopicCreate payload = (ClientMessagePayload_TopicCreate)message.payload;
			converted = MutationRecord.createTopic(termNumber, mutationOffsetToAssign, payload.topic, clientId, message.nonce, payload.code, payload.arguments);
		}
			break;
		case TOPIC_DESTROY: {
			ClientMessagePayload_TopicDestroy payload = (ClientMessagePayload_TopicDestroy)message.payload;
			converted = MutationRecord.destroyTopic(termNumber, mutationOffsetToAssign, payload.topic, clientId, message.nonce);
		}
			break;
		case KEY_PUT: {
			ClientMessagePayload_KeyPut payload = (ClientMessagePayload_KeyPut)message.payload;
			byte[] key = payload.key;
			byte[] value = payload.value;
			converted = MutationRecord.put(termNumber, mutationOffsetToAssign, payload.topic, clientId, message.nonce, key, value);
		}
			break;
		case KEY_DELETE: {
			ClientMessagePayload_KeyDelete payload = (ClientMessagePayload_KeyDelete)message.payload;
			byte[] key = payload.key;
			converted = MutationRecord.delete(termNumber, mutationOffsetToAssign, payload.topic, clientId, message.nonce, key);
		}
			break;
		case POISON: {
			ClientMessagePayload_KeyPut payload = (ClientMessagePayload_KeyPut)message.payload;
			byte[] key = payload.key;
			byte[] value = payload.value;
			converted = MutationRecord.put(termNumber, mutationOffsetToAssign, payload.topic, clientId, message.nonce, key, value);
		}
			break;
		case STUTTER: {
			ClientMessagePayload_KeyPut payload = (ClientMessagePayload_KeyPut)message.payload;
			byte[] key = payload.key;
			byte[] value = payload.value;
			converted = MutationRecord.stutter(termNumber, mutationOffsetToAssign, payload.topic, clientId, message.nonce, key, value);
		}
			break;
		case CONFIG_CHANGE: {
			// Eventually, this will kick-off the joint consensus where we change to having 2 active configs until this commits on all nodes and the local disk.
			// For now, however, we just send the received ack and enqueue this for commit (note that it DOES NOT generate an event - only a mutation).
			// The more complex operation happens after the commit completes since that is when we will change our state and broadcast the new config to all clients and listeners.
			ClusterConfig newConfig = ((ClientMessagePayload_ConfigChange)message.payload).config;
			converted = MutationRecord.updateConfig(termNumber, mutationOffsetToAssign, clientId, message.nonce, newConfig);
		}
			break;
		default:
			Assert.unimplemented("This is an invalid message for this client type and should be disconnected");
			break;
		}
		return converted;
	}
}
