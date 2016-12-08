package com.spark.core.java;


import scala.math.Ordered;
import java.io.Serializable;

/**
 * 自定义二次排序key
 * Created by TZQ on 2016/12/7.
 */
public class SecondarySortKey implements Ordered<SecondarySortKey>, Serializable {
    private int first;
    private int second;

    public SecondarySortKey(int first, int second) {
        this.first = first;
        this.second = second;
    }

    public void setFirst(int first) {
        this.first = first;
    }

    public void setSecond(int second) {
        this.second = second;
    }

    public int getFirst() {
        return first;
    }

    public int getSecond() {
        return second;
    }

    @Override
    public boolean equals(Object obj) {
        if(this==obj){
            return true;
        }
        if(obj==null){
            return false;
        }
        if(getClass()!=obj.getClass()){
            return false;
        }
        SecondarySortKey other=(SecondarySortKey)obj;
        if (first!=other.first){
            return false;
        }
        if(second!=other.second){
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        final int prime=33;
        int result=1;
        result=prime*result+first;
        result=prime*result+second;
        return  result;
    }

    public boolean $greater(SecondarySortKey other) {
        if (this.getFirst()>other.getFirst()){
            return true;
        }else if (this.first==other.getFirst()&& this.second>other.second){
            return true;
        }
        return false;
    }

    public boolean $greater$eq(SecondarySortKey other) {
        if(this.$greater(other)){
            return true;
        }else if (this.first==other.getFirst()&& this.second==other.getSecond()){
            return true;
        }
        return false;
    }

    public boolean $less(SecondarySortKey other) {
        if(this.first<other.getFirst()){
            return true;
        }else if (this.first==other.getFirst()&& this.second<other.getSecond()){
            return false;
        }
        return false;
    }

    public boolean $less$eq(SecondarySortKey other) {
        if(this.$less(other)){
            return true;
        }else if (this.first==other.getFirst()&& this.second==other.getSecond()){
            return true;
        }
        return false;
    }

    public int compare(SecondarySortKey other) {
        if (this.first-other.getFirst()!=0){
            return this.first-other.getFirst();
        }else {
            return this.second-other.getSecond();
        }
    }

    public int compareTo(SecondarySortKey other) {
        if (this.first-other.getFirst()!=0){
            return this.first-other.getFirst();
        }else {
            return this.second-other.getSecond();
        }
    }
}
