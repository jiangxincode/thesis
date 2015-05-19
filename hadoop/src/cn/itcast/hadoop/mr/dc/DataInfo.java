package cn.itcast.hadoop.mr.dc;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class DataInfo implements Writable{

	private String tel;
	private long upPayLoad;
	private long downPayLoad;
	private long totalPayLoad;
	
	public DataInfo(){}
	
	public DataInfo(String tel, long upPayLoad, long downPayLoad) {
		this.tel = tel;
		this.upPayLoad = upPayLoad;
		this.downPayLoad = downPayLoad;
		this.totalPayLoad = upPayLoad + downPayLoad;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(tel);
		out.writeLong(upPayLoad);
		out.writeLong(downPayLoad);
		out.writeLong(totalPayLoad);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.tel = in.readUTF();
		this.upPayLoad = in.readLong();
		this.downPayLoad = in.readLong();
		this.totalPayLoad = in.readLong();
		
	}

	@Override
	public String toString() {
		return upPayLoad + "\t" + downPayLoad + "\t" + totalPayLoad;
	}

	public String getTel() {
		return tel;
	}

	public void setTel(String tel) {
		this.tel = tel;
	}

	public long getUpPayLoad() {
		return upPayLoad;
	}

	public void setUpPayLoad(long upPayLoad) {
		this.upPayLoad = upPayLoad;
	}

	public long getDownPayLoad() {
		return downPayLoad;
	}

	public void setDownPayLoad(long downPayLoad) {
		this.downPayLoad = downPayLoad;
	}

	public long getTotalPayLoad() {
		return totalPayLoad;
	}

	public void setTotalPayLoad(long totalPayLoad) {
		this.totalPayLoad = totalPayLoad;
	}

}
