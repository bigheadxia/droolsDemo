package com.coocaa.lux.kettle;

import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.util.EnvUtil;
import org.pentaho.di.job.Job;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.repository.RepositoryDirectoryInterface;
import org.pentaho.di.repository.kdr.KettleDatabaseRepository;
import org.pentaho.di.repository.kdr.KettleDatabaseRepositoryMeta;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Created with IDEA
 * User: LEIJIE
 * Date: 2017/5/23 0023
 * Time: 16:07
 */
public class demo {

    //通过文件方式执行转换
    public static void runTransfer(String[] params, String ktrPath) {
        Trans trans = null;
        try {
            // // 初始化
            // 转换元对象
            KettleEnvironment.init();// 初始化
            EnvUtil.environmentInit();
            TransMeta transMeta = new TransMeta(ktrPath);
            // 转换
            trans = new Trans(transMeta);

            // 执行转换
            trans.execute(params);
            // 等待转换执行结束
            trans.waitUntilFinished();
            // 抛出异常
            if (trans.getErrors() > 0) {
                throw new Exception("There are errors during transformation exception!(传输过程中发生异常)");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //通过文件方式执行job
    public static void runJob(Map<String,String> maps, String jobPath) {
        try {
            KettleEnvironment.init();
            // jobname 是Job脚本的路径及名称
            JobMeta jobMeta = new JobMeta(jobPath, null);
            Job job = new Job(null, jobMeta);
            // 向Job 脚本传递参数，脚本中获取参数值：${参数名}
            // job.setVariable(paraname, paravalue);
            Set<Map.Entry<String, String>> set=maps.entrySet();
            for(Iterator<Map.Entry<String, String>> it = set.iterator(); it.hasNext();){
                Map.Entry<String, String> ent=it.next();
                job.setVariable(ent.getKey(), ent.getValue());
            }
            job.start();
            job.waitUntilFinished();
            if (job.getErrors() > 0) {
                throw new Exception(
                        "There are errors during job exception!(执行job发生异常)");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //执行资源库的中的转换
    public static void runWithDb() throws KettleException {
        KettleEnvironment.init();
        //创建DB资源库
        KettleDatabaseRepository repository=new KettleDatabaseRepository();
        DatabaseMeta databaseMeta=new DatabaseMeta("kettle","mysql","jdbc","192.168.3.251","etl_test","3306","root","root");
        //选择资源库
        KettleDatabaseRepositoryMeta kettleDatabaseRepositoryMeta=new KettleDatabaseRepositoryMeta("kettle","kettle","Transformation description",databaseMeta);
        repository.init(kettleDatabaseRepositoryMeta);
        //连接资源库
        repository.connect("root","root");
        RepositoryDirectoryInterface directoryInterface=repository.loadRepositoryDirectoryTree();
        //选择转换
        TransMeta transMeta=repository.loadTransformation("demo1",directoryInterface,null,true,null);
        Trans trans=new Trans(transMeta);
        trans.execute(null);
        trans.waitUntilFinished();//等待直到数据结束
        if(trans.getErrors()>0){
            System.out.println("transformation error");
        }else{
            System.out.println("transformation successfully");
        }
    }

    public static void main(String[] args) {
        runJob(new HashMap<String, String>(),"G:\\etltest\\执行.kjb");
    }
}
