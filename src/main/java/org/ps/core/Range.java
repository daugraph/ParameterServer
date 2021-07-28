package org.ps.core;

/** 左闭右开 [begin, end) */
public class Range {
    private int begin;
    private int end;

    public Range() {}

    public Range(int begin, int end) {
        this.begin = begin;
        this.end = end;
    }

    public int getBegin() {
        return begin;
    }

    public int getEnd() {
        return end;
    }

    public int size() {
        return end - begin;
    }
}
