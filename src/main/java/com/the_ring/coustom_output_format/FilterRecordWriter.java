package com.the_ring.coustom_output_format;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class FilterRecordWriter extends RecordWriter<Text, NullWritable> {

    FSDataOutputStream mainOut = null;
    FSDataOutputStream otherOut = null;

    public FilterRecordWriter(TaskAttemptContext job) {

        FileSystem fs;

        try {
            fs = FileSystem.get(job.getConfiguration());

            Path mainPath = new Path("./atguigu.log");
            Path otherPath = new Path("./other.log");

            mainOut = fs.create(mainPath);
            otherOut = fs.create(otherPath);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void write(Text text, NullWritable nullWritable) throws IOException, InterruptedException {
        if (text.toString().contains("atguigu")) {
            mainOut.write(text.toString().getBytes());
        } else {
            otherOut.write(text.toString().getBytes());
        }
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        IOUtils.closeStream(mainOut);
        IOUtils.closeStream(otherOut);
    }
}
