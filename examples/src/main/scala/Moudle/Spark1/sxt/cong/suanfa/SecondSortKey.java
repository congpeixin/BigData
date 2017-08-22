package Moudle.Spark1.sxt.cong.suanfa;

import java.io.Serializable;

import scala.math.Ordered;

public class SecondSortKey implements Ordered<SecondSortKey>, Serializable{

	private static final long serialVersionUID = -5175775819070099285L;

	// 首先定义需要排序的列,为需要排序的列提供getter和setter和hashcode和equals方法
	private int first;
	private int second;
	
	public SecondSortKey(int first, int second) {
		this.first = first;
		this.second = second;
	}

	public int getFirst() {
		return first;
	}

	public void setFirst(int first) {
		this.first = first;
	}

	public int getSecond() {
		return second;
	}

	public void setSecond(int second) {
		this.second = second;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + first;
		result = prime * result + second;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		SecondSortKey other = (SecondSortKey) obj;
		if (first != other.first)
			return false;
		if (second != other.second)
			return false;
		return true;
	}

	@Override
	public boolean $greater(SecondSortKey other) {
		// 要去定义当前key在什么情况下是大于其它key
		if(this.first > other.getFirst()){
			return true;
		}else if(this.first == other.getFirst() && this.second > other.getSecond()){
			return true;
		}
		return false;
	}

	@Override
	public boolean $greater$eq(SecondSortKey other) {
		if(this.$greater(other)){
			return true;
		}else if(this.first == other.getFirst() && this.second == other.getSecond()){
			return true;
		}
		return false;
	}

	@Override
	public boolean $less(SecondSortKey other) {
		if(this.first < other.getFirst()){
			return true;
		}else if(this.first == other.getFirst() && this.second < other.getSecond()){
			return true;
		}
		return false;
	}

	@Override
	public boolean $less$eq(SecondSortKey other) {
		if(this.$less(other)){
			return true;
		}else if(this.first == other.getFirst() && this.second == other.getSecond()){
			return true;
		}
		return false;
	}

	@Override
	public int compare(SecondSortKey other) {
		if(this.first - other.getFirst() != 0){
			return this.first - other.getFirst();
		}else {
			return this.second - other.getSecond();
		}
	}

	@Override
	public int compareTo(SecondSortKey other) {
		if(this.first - other.getFirst() != 0){
			return this.first - other.getFirst();
		}else {
			return this.second - other.getSecond();
		}
	}
}
