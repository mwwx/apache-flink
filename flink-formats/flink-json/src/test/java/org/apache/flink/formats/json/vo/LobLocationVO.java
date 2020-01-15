package org.apache.flink.formats.json.vo;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Log Information for JSON.
 */
public class LobLocationVO implements Serializable {

	long ldbBlobId;
	long pallasBlobId;
	String nodeId;
	String tblName;
	long shardId;
	String txn;
	long sn;
	List<String> nodeIds;

	// for original ldb
	public int blockAddress;

	public LobLocationVO() {
		this(0);
	}

	public LobLocationVO(int blockAddress){
		this.blockAddress = blockAddress;
	}

	public LobLocationVO(long ldbBlobId, long pallasBlobId, String nodeId, String tblName, long shardId, String txn, long sn, List<String> nodeIds) {
		this.ldbBlobId = ldbBlobId;
		this.pallasBlobId = pallasBlobId;
		this.nodeId = nodeId;
		this.tblName = tblName;
		this.shardId = shardId;
		this.txn = txn;
		this.sn = sn;
		this.nodeIds = nodeIds;
	}

	public long getLdbBlobId() {
		return ldbBlobId;
	}

	public void setLdbBlobId(long ldbBlobId) {
		this.ldbBlobId = ldbBlobId;
	}

	public long getPallasBlobId() {
		return pallasBlobId;
	}

	public void setPallasBlobId(long pallasBlobId) {
		this.pallasBlobId = pallasBlobId;
	}

	public String getNodeId() {
		return nodeId;
	}

	public void setNodeId(String nodeId) {
		this.nodeId = nodeId;
	}

	public String getTblName() {
		return tblName;
	}

	public void setTblName(String tblName) {
		this.tblName = tblName;
	}

	public long getShardId() {
		return shardId;
	}

	public void setShardId(long shardId) {
		this.shardId = shardId;
	}

	public String getTxn() {
		return txn;
	}

	public void setTxn(String txn) {
		this.txn = txn;
	}

	public long getSn() {
		return sn;
	}

	public void setSn(long sn) {
		this.sn = sn;
	}

	public List<String> getNodeIds() {
		return nodeIds;
	}

	public void setNodeIds(List<String> nodeIds) {
		this.nodeIds = nodeIds;
	}

	public int getBlockAddress() {
		return blockAddress;
	}

	public void setBlockAddress(int blockAddress) {
		this.blockAddress = blockAddress;
	}

	public byte[] serialize() {
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		DataOutputStream dataOutputStream = new DataOutputStream(bos);
		try {
			dataOutputStream.writeLong(pallasBlobId);
			dataOutputStream.writeUTF(nodeId);
			dataOutputStream.writeUTF(tblName);
			dataOutputStream.writeLong(shardId);
			dataOutputStream.writeUTF(txn);
			dataOutputStream.writeLong(sn);
			dataOutputStream.writeInt(nodeIds.size());
			for (int i = 0; i < nodeIds.size(); i++){
				dataOutputStream.writeUTF(nodeIds.get(i));
			}
			return bos.toByteArray();

		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public static LobLocationVO deserialize(byte[] bytes) {
		ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
		DataInputStream ins = new DataInputStream(bis);

		try {
			long pallasBlobId = ins.readLong();
			String nodeId = ins.readUTF();
			String tblName = ins.readUTF();
			long shardId = ins.readLong();
			String txn = ins.readUTF();
			long sn = ins.readLong();
			List<String> nodeIds = new ArrayList<>();
			int size = ins.readInt();
			for (int i = 0; i < size; i++){
				nodeIds.add(ins.readUTF());
			}
			return new LobLocationVO(0, pallasBlobId, nodeId, tblName, shardId, txn, sn, nodeIds);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

	}
}
