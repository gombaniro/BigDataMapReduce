package org.example.organization;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;

public class LastAccessDatePartitioner extends Partitioner<IntWritable, Text>
        implements Configurable {

    private static final String MIN_LAST_ACCESS_DATE_YEAR =
            "min.last.access.date.year";

    private Configuration conf = null;
    private int minLastAccessDateYear = 0;
    @Override
    public void setConf(Configuration configuration) {
        this.conf = configuration;
        minLastAccessDateYear = conf.getInt(MIN_LAST_ACCESS_DATE_YEAR, 0);
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    public int getPartition(IntWritable key, Text text, int numPartitions) {
        return  key.get() - minLastAccessDateYear;
    }

    public static void setMinLastAccessDateYear(Job job, int minLastAccessDateYear) {
        job.getConfiguration().setInt(MIN_LAST_ACCESS_DATE_YEAR, minLastAccessDateYear);
    }
}
