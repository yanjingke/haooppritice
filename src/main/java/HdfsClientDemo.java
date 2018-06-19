import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.util.Iterator;
import java.util.Map;


public class HdfsClientDemo {
    FileSystem fs=null;
    Configuration conf=null;

    public static void main(String[] args) {

    }
    @Before
    public void init () throws Exception {

     conf=new Configuration();
        conf.set("dfs.replication", "5");
        fs=FileSystem.get(conf);
        fs=FileSystem.get(new URI("hdfs://172.20.134.171:9000"),conf,"root");
    }
    @Test
    public void testConf(){
        Iterator<Map.Entry<String, String>> it= conf.iterator();
        while (it.hasNext()){
           Map.Entry<String,String> ent=it.next();
           System.out.println(ent.getKey()+":"+ent.getValue());
        }

    }
    @Test
    public void testUpload() throws Exception {

        Thread.sleep(2000);
        fs.copyFromLocalFile(new Path("E:/sn.txt"), new Path("/"));
        fs.close();
    }

    @Test
    public void testMkdir() throws Exception {
        boolean mkdir= fs.mkdirs(new Path("/HH/DSADSA/DSADSADSA/DSADASDSAADS"));
        System.out.println("jdjjd"+mkdir);
    }
    @Test
    public void testDeleteMk() throws IOException {
        boolean T= fs.delete(new Path("/HH"),true);
        System.out.println("jdjjd"+T);
    }
    @Test //
    public void testLs() throws IOException {

        RemoteIterator<LocatedFileStatus> listFiles= fs.listFiles(new Path("/"), true);
        while (listFiles.hasNext()){
            LocatedFileStatus fileStatus = listFiles.next();
            System.out.println("BLOCKSIZE"+fileStatus.getBlockSize());
            System.out.println("ower"+fileStatus.getOwner());
            System.out.println("Permission"+fileStatus.getPermission());
            System.out.println("NAME"+fileStatus.getPath().getName());//递归列出子文件夹的文件
            System.out.println("Replication"+fileStatus.getReplication());

            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            for(BlockLocation b:blockLocations){
                System.out.println("长度：+"+b.getLength());
                System.out.println("块偏移量：+"+b.getOffset());
                String[]dataNode = b.getHosts();
                for (String dn:dataNode)
                System.out.println("块所在地址：+"+dn);
            }
            System.out.println("----------------------------------------------");

        }
    }
    @Test
    public void testLs2() throws IOException {
        FileStatus[] fileStatuses = fs.listStatus(new Path("/"));
        for(FileStatus file:fileStatuses){
            System.out.println("name"+file.getPath().getName());
            System.out.println(file.isFile()?"file":"directory");
        }

    }

}
