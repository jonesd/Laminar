package com.jeffdisher.laminar.console;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.function.Consumer;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.jeffdisher.laminar.state.F;
import com.jeffdisher.laminar.state.StateSnapshot;


public class TestConsoleManager {
	private PrintStream _fakeOut;

	@Before
	public void beforeEach() {
		// Create a new fake sink for every test.
		_fakeOut = new PrintStream(new ByteArrayOutputStream(1024));
	}

	@Test
	public void testStopParse() throws Throwable {
		FakeStream stream = new FakeStream("stop\n");
		F<Consumer<StateSnapshot>> wrapper = new F<>();
		boolean[] out = new boolean[1];
		ConsoleManager manager = new ConsoleManager(_fakeOut, stream, new IConsoleManagerBackgroundCallbacks() {
			@Override
			public void ioEnqueueConsoleCommandForMainThread(Consumer<StateSnapshot> command) {
				wrapper.put(command);
			}
			@Override
			public void mainHandleStopCommand() {
				out[0] = true;
			}
		});
		manager.startAndWaitForReady();
		stream.unblock();
		wrapper.get().accept(null);
		Assert.assertTrue(out[0]);
		manager.stopAndWaitForTermination();
	}

	@Test
	public void testStopWithArgs() throws Throwable {
		FakeStream stream = new FakeStream("stop now in 5\n");
		F<Consumer<StateSnapshot>> wrapper = new F<>();
		boolean[] out = new boolean[1];
		ConsoleManager manager = new ConsoleManager(_fakeOut, stream, new IConsoleManagerBackgroundCallbacks() {
			@Override
			public void ioEnqueueConsoleCommandForMainThread(Consumer<StateSnapshot> command) {
				wrapper.put(command);
			}
			@Override
			public void mainHandleStopCommand() {
				out[0] = true;
			}
		});
		manager.startAndWaitForReady();
		stream.unblock();
		wrapper.get().accept(null);
		Assert.assertTrue(out[0]);
		manager.stopAndWaitForTermination();
	}

	@Test
	public void testNoInput() throws Throwable {
		FakeStream stream = new FakeStream("");
		ConsoleManager manager = new ConsoleManager(_fakeOut, stream, new IConsoleManagerBackgroundCallbacks() {
			@Override
			public void ioEnqueueConsoleCommandForMainThread(Consumer<StateSnapshot> command) {
				// We shouldn't see this.
				Assert.fail();
			}
			@Override
			public void mainHandleStopCommand() {
				// We shouldn't see this.
				Assert.fail();
			}
		});
		manager.startAndWaitForReady();
		stream.unblock();
		// Wait for a second and then just stop.
		Thread.sleep(1000L);
		manager.stopAndWaitForTermination();
	}

	@Test
	public void testStopMultiple() throws Throwable {
		FakeStream stream = new FakeStream("stop\nstopstop\nstop\n\n\n");
		F<Consumer<StateSnapshot>> wrapper = new F<>();
		boolean[] out = new boolean[1];
		ConsoleManager manager = new ConsoleManager(_fakeOut, stream, new IConsoleManagerBackgroundCallbacks() {
			@Override
			public void ioEnqueueConsoleCommandForMainThread(Consumer<StateSnapshot> command) {
				wrapper.put(command);
			}
			@Override
			public void mainHandleStopCommand() {
				out[0] = true;
			}
		});
		manager.startAndWaitForReady();
		stream.unblock();
		wrapper.get().accept(null);
		Assert.assertTrue(out[0]);
		manager.stopAndWaitForTermination();
	}

	@Test
	public void testLongCommand() throws Throwable {
		StringBuilder builder = new StringBuilder();
		for (int i = 0; i < 2048; ++i) {
			builder.append("a");
		}
		builder.append("\nstop\n");
		FakeStream stream = new FakeStream(builder.toString());
		F<Consumer<StateSnapshot>> wrapper = new F<>();
		boolean[] out = new boolean[1];
		ConsoleManager manager = new ConsoleManager(_fakeOut, stream, new IConsoleManagerBackgroundCallbacks() {
			@Override
			public void ioEnqueueConsoleCommandForMainThread(Consumer<StateSnapshot> command) {
				wrapper.put(command);
			}
			@Override
			public void mainHandleStopCommand() {
				out[0] = true;
			}
		});
		manager.startAndWaitForReady();
		stream.unblock();
		wrapper.get().accept(null);
		Assert.assertTrue(out[0]);
		manager.stopAndWaitForTermination();
	}


	private static class FakeStream extends InputStream {
		private final byte[] _bytes;
		private int _index;
		private boolean _shouldBlock;
		
		public FakeStream(String stringToReturn) {
			_bytes = stringToReturn.getBytes();
			_index = 0;
			_shouldBlock = true;
		}
		
		public synchronized void unblock() {
			_shouldBlock = false;
			this.notifyAll();
		}
		
		@Override
		public synchronized int available() throws IOException {
			return _shouldBlock
					? 0
					: (_bytes.length - _index);
		}
		
		@Override
		public synchronized int read() throws IOException {
			// We should never read this when we are blocking.
			Assert.assertFalse(_shouldBlock);
			// We should be returning no bytes available after reading the buffer.
			Assert.assertTrue(_index < _bytes.length);
			
			int value = _bytes[_index];
			_index += 1;
			return value;
		}
	}
}
