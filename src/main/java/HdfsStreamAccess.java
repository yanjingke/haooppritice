import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
//实现指定文件的读取
public class HdfsStreamAccess {
    FileSystem fs=null;
    Configuration conf=null;
    @Before
    public void init () throws Exception {

        conf=new Configuration();
        conf.set("dfs.replication", "5");
        fs=FileSystem.get(conf);
        fs=FileSystem.get(new URI("hdfs://172.20.134.171:9000"),conf,"root");
    }
    @Test
    //通过流的方式获取上传文件
    public void testUpload() throws IOException {
        FSDataOutputStream outputStream = fs.create(new Path("/x"), true);
        FileInputStream inputStream = new FileInputStream("d:/sn.txt");
        IOUtils.copy(inputStream,outputStream);
    }
    @Test//流下载
    public void testDownLoad() throws IOException {
        FSDataInputStream inputStream = fs.open(new Path("/x"));
        FileOutputStream fileOutputStream = new FileOutputStream("E:/HdfsClientDemo.class");
        IOUtils.copy(inputStream,fileOutputStream);

    }
    @Test //指定12字节后读取
    public void testRandomeAccess() throws IOException {
        FSDataInputStream inputStream= fs.open(new Path("/x"));
        inputStream.seek(12);
        FileOutputStream fileOutputStream = new FileOutputStream("E:/HdfsClientDemo2.class");
        IOUtils.copy(inputStream,fileOutputStream);
    }
    @Test //文件打到屏幕上
    public void testCat() throws IOException {
        FSDataInputStream inputStream= fs.open(new Path("/x"));
        IOUtils.copy(inputStream,System.out);
    }

}
