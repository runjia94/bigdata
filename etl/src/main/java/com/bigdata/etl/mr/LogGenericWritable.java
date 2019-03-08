package com.bigdata.etl.mr;


import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

//通用日志数据传输类
public abstract class LogGenericWritable implements Writable {
    private LogFieldWritable[] datum;
    private String[] name;
    private Map<String, Integer> nameIndex;

    abstract protected String[] getFieldName();

    public LogGenericWritable(){
        name = getFieldName();
        // check duplicate name
        if(name == null){
            throw new RuntimeException("The field names can not be null");

        }
        nameIndex = new HashMap<String, Integer>();
        for(int index = 0; index< name.length; index++){
            if(nameIndex.containsKey(name[index])){
                throw new RuntimeException("The field"+name[index]+"duplicate");
            }

            nameIndex.put(name[index], index);
        }

        //init data
        datum = new LogFieldWritable[name.length];
        for(int i = 0; i<name.length; i++){
            datum[i] = new LogFieldWritable();
        }
    }


    public void put(String name, LogFieldWritable value){
        int index = getIndexWithName(name);
        datum[index] = value;
    }

    public LogFieldWritable getWritable(String name){
        int index = getIndexWithName(name);
        return datum[index];
    }

    public Object getObject(String name){
        return getWritable(name).getObject();
    }

    private int getIndexWithName(String name){
        Integer index = nameIndex.get(name);
        if(index == null){
            throw new RuntimeException("The field" + name + "not registered");

        }
        return index;
    }

    //序列化与反序列化
    public void write(DataOutput out) throws IOException {
        WritableUtils.writeVInt(out, name.length);
        for(int i = 0; i<name.length; i++){
            datum[i].write(out);
        }
    }

    public void readFields(DataInput in) throws IOException {
        int length = WritableUtils.readVInt(in);
        datum = new LogFieldWritable[length];
        for(int i = 0; i < length; i++){
            LogFieldWritable value = new LogFieldWritable();
            value.readFields(in);
            datum[i] = value;
        }

    }

    public String asJsonString(){
        JSONObject json = new JSONObject();
        for(int i = 0; i < name.length; i++){
            json.put(name[i], datum[i].getObject());
        }

        return json.toJSONString();
    }

}

