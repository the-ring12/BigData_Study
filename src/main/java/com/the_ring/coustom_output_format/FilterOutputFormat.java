package com.the_ring.coustom_output_format;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.*;
import org.w3c.dom.Text;

import java.io.IOException;

/**
 * 自定义 OutputFormat
 */
public class FilterOutputFormat extends OutputFormat<Text, NullWritable> {

    @Override
    public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return null;
    }

    @Override
    public void checkOutputSpecs(JobContext jobContext) throws IOException, InterruptedException {

    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return null;
    }
}
