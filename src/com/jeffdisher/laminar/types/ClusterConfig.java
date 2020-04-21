package com.jeffdisher.laminar.types;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;

import com.jeffdisher.laminar.utils.Assert;


/**
 * Represents the description of a coherent cluster of machines.
 * This includes the IP:port of both the cluster-facing and client-facing port (no assumption that they have the same
 * IP).
 * The config is just data describing the cluster and doesn't change based on who is leader or which nodes are
 * online/offline.
 * The config can be serialized/deserialized for movement over the wire and support both IPv4 and IPv6 configurations.
 * Equality and hash are defined on the entries, but not the entire config, itself.  This is because the equality of
 * entire configs is not actually of importance (outside of testing) but tracking the changes of individual nodes is a
 * common use-case (since partial overlap defines how to manage joint consensus).
 * Note that, while in a "joint consensus" state, the nodes will be handling 2 ClusterConfig instances.
 */
public final class ClusterConfig {
	public static final int MAX_CLUSTER_MEMBERS = 31;
	public static final int IPV4_BYTE_SIZE = 4;
	public static final int IPV6_BYTE_SIZE = 16;
	public static final int MAX_PORT = (64 * 1024) - 1;

	/**
	 * Creates a new config from a list of entries.  Note that the InetSocketAddress instances in the entries will be
	 * cleaned of hostnames to make future comparisons more easily defined.
	 * 
	 * @param entries The entries in the config.
	 * @return A new ClusterConfig instance.
	 */
	public static ClusterConfig configFromEntries(ConfigEntry[] entries) {
		if ((entries.length <= 0) || (entries.length > MAX_CLUSTER_MEMBERS)) {
			throw _parseError();
		}
		// We want to ensure that we have cleaned all the socket address instances to ensure equality is properly defined.
		ConfigEntry[] copy = new ConfigEntry[entries.length];
		for (int i = 0; i < entries.length; ++i) {
			InetSocketAddress cluster = _cleanSocketAddress(entries[i].cluster);
			InetSocketAddress client = _cleanSocketAddress(entries[i].client);
			copy[i] = new ConfigEntry(cluster, client);
		}
		return new ClusterConfig(copy);
	}

	/**
	 * Creates a "cleaned" version of the input InetSocketAddress.  Specifically, this means that any hostnames are
	 * removed and only the IP address and port remain.
	 * 
	 * @param input The InetSocketAddress to clean.
	 * @return A new InetSocketAddress instance composed of only the IP address and port of the input.
	 */
	public static InetSocketAddress cleanSocketAddress(InetSocketAddress input) {
		return _cleanSocketAddress(input);
	}

	/**
	 * Creates a new ClusterConfig instance from a previously serialized instance.
	 * 
	 * @param serialized The raw bytes of a previously serialized ClusterConfig.
	 * @return A new ClusterConfig instance.
	 */
	public static ClusterConfig deserialize(byte[] serialized) {
		ByteBuffer buffer = ByteBuffer.wrap(serialized);
		return _deserializeFrom(buffer);
	}

	/**
	 * Creates a new ClusterConfig instance from a previously serialized instance.
	 * This method has the side-effect of advancing the cursor in the given buffer.
	 * 
	 * @param buffer A ByteBuffer containing a previously serialized ClusterConfig.
	 * @return A new ClusterConfig instance.
	 */
	public static ClusterConfig deserializeFrom(ByteBuffer serialized) {
		return _deserializeFrom(serialized);
	}


	private static InetSocketAddress _cleanSocketAddress(InetSocketAddress input) {
		try {
			return new InetSocketAddress(InetAddress.getByAddress(input.getAddress().getAddress()), input.getPort());
		} catch (UnknownHostException e) {
			// This can't happen when directly converting one instance to another.
			throw Assert.unexpected(e);
		}
	}

	private static ClusterConfig _deserializeFrom(ByteBuffer buffer) {
		byte entryCount = buffer.get();
		if ((entryCount <= 0) || (entryCount > MAX_CLUSTER_MEMBERS)) {
			throw _parseError();
		}
		ConfigEntry[] entries = new ConfigEntry[entryCount];
		for (int i = 0; i < entries.length; ++i) {
			InetSocketAddress cluster = _readPair(buffer);
			InetSocketAddress client = _readPair(buffer);
			entries[i] = new ConfigEntry(cluster, client);
		}
		return new ClusterConfig(entries);
	}


	public final ConfigEntry[] entries;

	private ClusterConfig(ConfigEntry[] entries) {
		this.entries = entries;
	}

	/**
	 * Determines the number of bytes required to serialize the receiver.
	 * 
	 * @return The number of bytes occupied by the serialized receiver.
	 */
	public int serializedSize() {
		return _serializedSize();
	}

	/**
	 * Serializes the receiver into the given buffer.
	 * Note that this has the side-effect of advancing the cursor in the given buffer.
	 * 
	 * @param buffer The buffer which will be populated by the serialized receiver.
	 */
	public void serializeInto(ByteBuffer buffer) {
		_serializeInto(buffer);
	}

	/**
	 * Serializes the receiver into raw bytes.
	 * 
	 * @return The raw byte serialization of the receiver.
	 */
	public byte[] serialize() {
		int bufferSize = _serializedSize();
		ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
		_serializeInto(buffer);
		// The size is precise so just return the underlying array.
		return buffer.array();
	}


	private static IllegalArgumentException _parseError() {
		throw new IllegalArgumentException("ClusterConfig invalid");
	}

	private static InetSocketAddress _readPair(ByteBuffer buffer) {
		byte ipLength = buffer.get();
		if ((IPV4_BYTE_SIZE != ipLength) && (IPV6_BYTE_SIZE != ipLength)) {
			throw _parseError();
		}
		byte[] ip = new byte[ipLength];
		buffer.get(ip);
		int port = Short.toUnsignedInt(buffer.getShort());
		try {
			return new InetSocketAddress(InetAddress.getByAddress(ip), port);
		} catch (UnknownHostException e) {
			// Only happens if the IP is the incorrect length and we already checked this.
			throw Assert.unexpected(e);
		}
	}

	private void _writePair(ByteBuffer buffer, InetSocketAddress pair) {
		byte[] ip = pair.getAddress().getAddress();
		// These are the sizes defined within the InetAddress documentation
		Assert.assertTrue((IPV4_BYTE_SIZE == ip.length) || (IPV6_BYTE_SIZE == ip.length));
		short port = (short)pair.getPort();
		byte ipLength = (byte)ip.length;
		buffer.put(ipLength);
		buffer.put(ip);
		buffer.putShort(port);
	}

	private int _serializedSize() {
		// We have 1 byte for the number of entries but each entry can be a different size.
		int bufferSize = Byte.BYTES;
		for (ConfigEntry entry : this.entries) {
			// The port is always a u16 but the IP can be 4 or 16 bytes, and each one has a byte to describe which.
			int clusterIpSize = entry.cluster.getAddress().getAddress().length;
			int clientIpSize = entry.client.getAddress().getAddress().length;
			bufferSize += Byte.BYTES + clusterIpSize + Byte.BYTES + clientIpSize + (2 * Short.BYTES);
		}
		return bufferSize;
	}

	private void _serializeInto(ByteBuffer buffer) {
		byte entryCount = (byte)this.entries.length;
		buffer.put(entryCount);
		for (ConfigEntry entry : this.entries) {
			_writePair(buffer, entry.cluster);
			_writePair(buffer, entry.client);
		}
	}


	/**
	 * A single entry in the ClusterConfig, representing a single node.
	 * Note that the cluster-facing and client-facing sockets are defined independently.
	 */
	public static final class ConfigEntry {
		public final InetSocketAddress cluster;
		public final InetSocketAddress client;
		
		public ConfigEntry(InetSocketAddress cluster, InetSocketAddress client) {
			this.cluster = cluster;
			this.client = client;
		}
		
		@Override
		public boolean equals(Object obj) {
			boolean isEqual = false;
			if ((null != obj) && (ConfigEntry.class == obj.getClass())) {
				ConfigEntry other = (ConfigEntry) obj;
				isEqual = this.cluster.equals(other.cluster) && this.client.equals(other.client);
			}
			return isEqual;
		}
		
		@Override
		public int hashCode() {
			return this.cluster.hashCode() ^ this.client.hashCode();
		}
		
		@Override
		public String toString() {
			return "(Cluster: " + this.cluster.toString() + ", Client: " + this.client + ")";
		}
	}
}
