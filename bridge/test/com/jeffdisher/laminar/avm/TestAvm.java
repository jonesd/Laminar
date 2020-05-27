package com.jeffdisher.laminar.avm;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.List;
import java.util.UUID;

import org.aion.avm.utilities.JarBuilder;
import org.junit.Assert;
import org.junit.Test;

import com.jeffdisher.laminar.types.TopicName;
import com.jeffdisher.laminar.types.event.EventRecord;
import com.jeffdisher.laminar.types.event.EventRecordPayload_Put;


public class TestAvm {
	private static final String TEST_JAR_LOCATION = "/tmp/nothing.jar";

	@Test
	public void testStartStop() throws Throwable {
		try (RandomAccessFile file = new RandomAccessFile(TEST_JAR_LOCATION, "rw")) {
			byte[] jar = _getClassInJar(DoNothing.class);
			file.write(jar);
		}
		// We just write out this JAR for testing later on (since integration can't use this AVM helper.
		AvmBridge bridge = new AvmBridge();
		bridge.start();
		bridge.shutdown();
	}

	@Test
	public void testSimpleDeployment() throws Throwable {
		byte[] key = new byte[32];
		key[31] = 1;
		byte[] code = _getClassInJar(SimpleDeployment.class);
		byte[] arguments = key;
		
		long termNumber = 1L;
		long globalOffset = 1L;
		long initialLocalOffset = 1L;
		UUID clientId = UUID.randomUUID();
		long clientNonce = 1L;
		
		TopicName topic = TopicName.fromString("test");
		AvmBridge bridge = new AvmBridge();
		bridge.start();
		
		List<EventRecord> records = bridge.runCreate(termNumber, globalOffset, initialLocalOffset, clientId, clientNonce, topic, code, arguments);
		Assert.assertEquals(1, records.size());
		Assert.assertEquals(0, ((EventRecordPayload_Put)records.get(0).payload).value[0]);
		
		bridge.shutdown();
	}

	@Test
	public void testDeploymentPutAndDelete() throws Throwable {
		byte[] key = new byte[32];
		key[31] = 2;
		byte[] code = _getClassInJar(SimpleDeployment.class);
		byte[] arguments = key;
		
		long termNumber = 1L;
		long globalOffset = 1L;
		long initialLocalOffset = 1L;
		UUID clientId = UUID.randomUUID();
		long clientNonce = 1L;
		
		TopicName topic = TopicName.fromString("test");
		byte[] value = new byte[] {1,2,3};
		AvmBridge bridge = new AvmBridge();
		bridge.start();
		
		List<EventRecord> records = bridge.runCreate(termNumber, globalOffset, initialLocalOffset, clientId, clientNonce, topic, code, arguments);
		Assert.assertEquals(1, records.size());
		Assert.assertEquals(0, ((EventRecordPayload_Put)records.get(0).payload).value[0]);
		globalOffset += 1;
		clientNonce += 1;
		initialLocalOffset += records.size();
		
		records = bridge.runPut(termNumber, globalOffset, initialLocalOffset, clientId, clientNonce, topic, key, value);
		Assert.assertEquals(1, records.size());
		Assert.assertEquals(1, ((EventRecordPayload_Put)records.get(0).payload).value[0]);
		globalOffset += 1;
		clientNonce += 1;
		initialLocalOffset += records.size();
		
		records = bridge.runDelete(termNumber, globalOffset, initialLocalOffset, clientId, clientNonce, topic, key);
		Assert.assertEquals(1, records.size());
		Assert.assertEquals(2, ((EventRecordPayload_Put)records.get(0).payload).value[0]);
		globalOffset += 1;
		clientNonce += 1;
		initialLocalOffset += records.size();
		
		bridge.shutdown();
	}


	private byte[] _getClassInJar(Class<?> clazz) throws IOException {
		// We can use the AVM utility for this.
		return JarBuilder.buildJarForExplicitMainAndClasses(clazz.getName(), clazz);
	}
}
