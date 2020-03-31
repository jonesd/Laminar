package com.jeffdisher.laminar;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;

import com.jeffdisher.laminar.console.ConsoleManager;
import com.jeffdisher.laminar.disk.DiskManager;
import com.jeffdisher.laminar.network.NetworkManager;
import com.jeffdisher.laminar.state.NodeState;


/**
 * The main class of a Laminar node.
 * Required command-line arguments:
 * -"--client" &lt;port&gt; - the port which will be bound for listening to client connections
 * -"--cluster" &lt;port&gt; - the port which will be bound for listening to connections from other cluster members
 * -"-data" - the directory which will be used for storing incoming and committed stream files
 * NOTE:  Port settings will be made optional in the future (mostly just for testing multiple nodes on one machine).
 * NOTE:  At some point, forcing interfaces for binding will be enabled but not in the short-term.
 */
public class Laminar {
	public static void main(String[] args) {
		// The way this starts up is to parse command-line options, bind required ports, output details of the start-up,
		// ensure the writability of the data directory, start all background components, print that start-up has
		// completed, and then the main thread transitions into its state management mode where it runs until shutdown.
		
		// Parse command-line options.
		String clientPortString = parseOption(args, "--client");
		String clusterPortString = parseOption(args, "--cluster");
		String dataDirectoryName = parseOption(args, "--data");
		
		// Make sure we were given our required options.
		if ((null == clientPortString) || (null == clusterPortString) || (null == dataDirectoryName)) {
			failStart("Missing options!  Usage:  Laminar --client <client_port> --cluster <cluster_port> --data <data_directory_path>");
		}
		
		// Parse ports.
		int clientPort = Integer.parseInt(clientPortString);
		int clusterPort = Integer.parseInt(clusterPortString);
		
		// Bind ports.
		ServerSocketChannel clientSocket = null;
		ServerSocketChannel clusterSocket = null;
		try {
			clientSocket = bindLocalPort(clientPort);
			clusterSocket = bindLocalPort(clusterPort);
		} catch (IOException e) {
			failStart("Failure binding required port: " + e.getLocalizedMessage());
		}
		
		// Check data directory.
		File dataDirectory = new File(dataDirectoryName);
		if (!dataDirectory.exists()) {
			boolean didCreate = dataDirectory.mkdirs();
			if (!didCreate) {
				failStart("Could not create data directory (or parents): \"" + dataDirectoryName +"\"");
			}
		}
		if (!dataDirectory.canWrite()) {
			failStart("Data directory not writable: \"" + dataDirectoryName +"\"");
		}
		
		// Log the successful start-up.
		System.out.println("Client-facing socket bound: " + clientPort);
		System.out.println("Cluster-facing socket bound: " + clusterPort);
		System.out.println("Data directory configured: " + dataDirectoryName);
		
		// By this point, all requirements of the system should be satisfied so create the subsystems.
		NetworkManager clientManager = new NetworkManager(clientSocket, null);
		NetworkManager clusterManager = new NetworkManager(clusterSocket, null);
		DiskManager disk = new DiskManager(dataDirectory);
		ConsoleManager console = new ConsoleManager(System.out, System.in, null);
		
		clientManager.startAndWaitForReady();
		clusterManager.startAndWaitForReady();
		disk.startAndWaitForReady();
		console.startAndWaitForReady();
		
		// We are now ready so enter the initial state.
		System.out.println("Laminar ready for leader connection or config upload...");
		NodeState thisNodeState = new NodeState(clientManager, clusterManager, disk, console);
		thisNodeState.runUntilShutdown();
		
		// The node state has entered a shutdown state so notify the user and close everything.
		System.out.println("Laminar shutting down...");
		clientManager.stopAndWaitForTermination();
		clusterManager.stopAndWaitForTermination();
		disk.stopAndWaitForTermination();
		console.stopAndWaitForTermination();
		
		// Close the resources we created - we just log if there are issues, and proceed.
		System.out.println("Laminar threads shutdown.  Closing sockets and terminating...");
		try {
			clientSocket.close();
		} catch (IOException e) {
			System.out.println("Client socket close exception: " + e.getLocalizedMessage());
		}
		try {
			clusterSocket.close();
		} catch (IOException e) {
			System.out.println("Cluster socket close exception: " + e.getLocalizedMessage());
		}
	}

	private static ServerSocketChannel bindLocalPort(int port) throws IOException {
		ServerSocketChannel socket = ServerSocketChannel.open();
		InetSocketAddress clientAddress = new InetSocketAddress(port);
		socket.bind(clientAddress);
		return socket;
	}

	private static void failStart(String message) {
		System.err.println("Fatal start-up error: " + message);
		throw new RuntimeException(message);
	}

	private static String parseOption(String[] args, String option) {
		String result = null;
		for (int index = 0; (null == result) && (index < (args.length - 1)); ++index) {
			if (option.equals(args[index])) {
				result = args[index+1];
			}
		}
		return result;
	}
}