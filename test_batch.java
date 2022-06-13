import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
//import java.io.BufferedInputStream;

import java.io.BufferedReader;

import java.io.File;

import java.io.FileInputStream;

import java.io.InputStreamReader;

//import java.io.Reader;

public class test_batch3 {
 
    public static void main(String[] args) {
        Connection conn = null;
        Statement stmt = null;
        Integer POOL_SIZE = 1000;
        try{

            List<Future<Integer>> fList = new ArrayList<>();
            ExecutorService es = Executors.newFixedThreadPool(POOL_SIZE);
            for(int i = 0; i < POOL_SIZE; ++i) {
                fList.add(es.submit(new con_work(i, POOL_SIZE, 20 )));
            }

            for(Future<Integer> f : fList) {
                try {
                    //if (f.get() !=0)
                    //  System.out.println(f.get());
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

           es.shutdown();
    
    
        }catch(Exception se){
            // 处理 JDBC 错误
            se.printStackTrace();
        }finally{
            // 关闭资源
            try{
                if(stmt!=null) stmt.close();
            }catch(SQLException se2){
            }// 什么都不做
            try{
                if(conn!=null) conn.close();
            }catch(SQLException se){
                se.printStackTrace();
            }
        }
        System.out.println("Goodbye!");
    
    }   
    
    public static class con_work implements Callable<Integer> {

        private int num;
        private int tot;
        private int bench_num;
        public con_work(int num, int tot, int bench_num){
            this.num = num;
            this.tot = tot;
            this.bench_num = bench_num;
        }
        // MySQL 8.0 以下版本 - JDBC 驱动名及数据库 URL
        static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";  
        static final String DB_URL1 = "jdbc:mysql://172.16.6.95:4101/jxjk?useCursorFetch=true&useServerPrepStmts=true&cachePrepStmts=true&useConfigs=maxPerformance&rewriteBatchedStatements=true";
        static final String DB_URL2 = "jdbc:mysql://172.16.6.63:4101/jxjk?useCursorFetch=true&useServerPrepStmts=true&cachePrepStmts=true&useConfigs=maxPerformance&rewriteBatchedStatements=true";
        static final String DB_URL3 = "jdbc:mysql://172.16.6.98:4101/jxjk?useCursorFetch=true&useServerPrepStmts=true&cachePrepStmts=true&useConfigs=maxPerformance&rewriteBatchedStatements=true";
        static final String DB_URL4 = "jdbc:mysql://172.16.6.62:4101/jxjk?useCursorFetch=true&useServerPrepStmts=true&cachePrepStmts=true&useConfigs=maxPerformance&rewriteBatchedStatements=true";
        static final String DB_URL5 = "jdbc:mysql://172.16.6.65:4101/jxjk?useCursorFetch=true&useServerPrepStmts=true&cachePrepStmts=true&useConfigs=maxPerformance&rewriteBatchedStatements=true";
        static final String DB_URL6 = "jdbc:mysql://172.16.6.66:4101/jxjk?useCursorFetch=true&useServerPrepStmts=true&cachePrepStmts=true&useConfigs=maxPerformance&rewriteBatchedStatements=true";
        static final String DB_URL7 = "jdbc:mysql://172.16.6.95:4201/jxjk?useCursorFetch=true&useServerPrepStmts=true&cachePrepStmts=true&useConfigs=maxPerformance&rewriteBatchedStatements=true";
        static final String DB_URL8 = "jdbc:mysql://172.16.6.63:4201/jxjk?useCursorFetch=true&useServerPrepStmts=true&cachePrepStmts=true&useConfigs=maxPerformance&rewriteBatchedStatements=true";
        static final String DB_URL9 = "jdbc:mysql://172.16.6.98:4201/jxjk?useCursorFetch=true&useServerPrepStmts=true&cachePrepStmts=true&useConfigs=maxPerformance&rewriteBatchedStatements=true";
        static final String DB_URL10 = "jdbc:mysql://172.16.6.62:4201/jxjk?useCursorFetch=true&useServerPrepStmts=true&cachePrepStmts=true&useConfigs=maxPerformance&rewriteBatchedStatements=true";
        static final String DB_URL11 = "jdbc:mysql://172.16.6.65:4201/jxjk?useCursorFetch=true&useServerPrepStmts=true&cachePrepStmts=true&useConfigs=maxPerformance&rewriteBatchedStatements=true";
        static final String DB_URL12 = "jdbc:mysql://172.16.6.66:4201/jxjk?useCursorFetch=true&useServerPrepStmts=true&cachePrepStmts=true&useConfigs=maxPerformance&rewriteBatchedStatements=true";
 
        // MySQL 8.0 以上版本 - JDBC 驱动名及数据库 URL
        //static final String JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";  
        //static final String DB_URL = "jdbc:mysql://localhost:3306/RUNOOB?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC";
        // 注册 JDBC 驱动
   
     
        // 数据库的用户名与密码，需要根据自己的设置
        static final String USER = "root";
        static final String PASS = "root";
        public Integer  call() throws ClassNotFoundException{
            Class.forName(JDBC_DRIVER);
            Connection conn = null;
            Statement stmt = null;
            try{
                String DB_URL = null;
                switch (num%12) {
                   case 0:
                       DB_URL = DB_URL1;
                       break;
                   case 1:
                       DB_URL = DB_URL2;
                       break;
                   case 2:
                       DB_URL = DB_URL3;
                       break;
                   case 3:
                       DB_URL = DB_URL4;
                       break;

                   case 4:
                       DB_URL = DB_URL5;
                       break;
                   case 5:
                       DB_URL = DB_URL6;
                       break;
                   case 6:
                       DB_URL = DB_URL7;
                       break;
                   case 7:
                       DB_URL = DB_URL8;
                       break;
                   case 8:
                       DB_URL = DB_URL9;
                       break;
                   case 9:
                       DB_URL = DB_URL10;
                       break;
                   case 10:
                       DB_URL = DB_URL11;
                       break;
                   case 11:
                       DB_URL = DB_URL12;
                       break;
             

                }
                // 打开链接
                System.out.println("连接数据库...");
                
                conn = DriverManager.getConnection(DB_URL,USER,PASS);
                conn.setAutoCommit(false);
                // 执行查询
                System.out.println(" 实例化Statement对象...");
                //String sql1 = "select * from TBPC1560 where MULTI_TENANCY_ID = 'CN000' and CST_ID = ? and RCRD_EXPY_TMS = '9999-12-31 23:59:59.999' " +
                //" and BSPD_ECD = 'ssd' and RETPCD = '0220001' and SRCSYS_AR_ID = ?";
                
                //String sql = "update TBPC1560 set CUR_STM_CRT_TMS = now() where MULTI_TENANCY_ID = 'CN000' and CST_ID = ? and RCRD_EXPY_TMS = '9999-12-31 23:59:59.999' " +
                //" and BSPD_ECD = 'ssd' and RETPCD = '0220001' and SRCSYS_AR_ID = ? ";
                String sql = "insert into TBPC1560_1  values(\"CN000\", ? ,\"9999-12-31 23:59:59.999\",\"ssd\",\"0220001\", ? , ? ,\"\",\"5423\",\"fdsvsdd\",\"N\",\"N\", ? ,\"ddd\",\"20200101\",\"22220202\",\"21210101\",\"555514751\",\"58772574\",\"555514751\",\"58772574\",\"2022-05-27 12:27:16\",\"2022-05-27 12:27:16\",\"20201111\",\"122151111\",\"457568\",\"578265\",?,\"2022-05-27 12:27:16\",53.3,\"20220121\")";
                PreparedStatement pstmt = conn.prepareStatement(sql);
                //PreparedStatement pstmt1 = conn.prepareStatement(sql1);
                BufferedReader bufferedReader = readTxtFile("./data19.csv");
                Statement statement = conn.createStatement();
                //statement.execute("set tidb_batch_insert = ON; ");
                //statement.execute("set tidb_dml_batch_size = 10; ");
               
                String lineTxt = null;
                for (int i = 0; i < num; i++) {
                    bufferedReader.readLine();  
                }
                lineTxt = bufferedReader.readLine();
                long start = System.currentTimeMillis();
                long end = start + 900000;
                long count = 0;
                long cos_time = 0;
                while(end > System.currentTimeMillis()) {
                    long start_time = System.currentTimeMillis();
                    for (int i = 0; i < bench_num; i++) {
                        String[] strs = lineTxt.split(",");
                        //System.out.println(strs[0]+",len:"+strs[0].length());
                        //System.out.println(strs[1]+",len:"+strs[1].length());
                        pstmt.setString(1, strs[0]);
                        pstmt.setString(2, strs[3]);
                        pstmt.setString(3, strs[2]);
                        pstmt.setString(4, strs[0]);
                        java.util.Date date=new java.util.Date();
                        pstmt.setTimestamp(5,new java.sql.Timestamp(date.getTime()));
                        //pstmt.addBatch();
                        //pstmt1.setString(1, strs[0]);
                        //pstmt1.setString(2, strs[1]);
                        //ResultSet rs = pstmt1.executeQuery();
                        // while(rs.next()){
                        //     //Thread.sleep(1000);
                        //     // 通过字段检索
                        //     String id  = rs.getString("CST_ID");
                        //     // 输出数据
                        //     System.out.print("ID: " + id);
                        // }
                        // 完成后关闭
                        //rs.close();
                        pstmt.executeUpdate();
                        for (int j = 0; j < tot; j++) {
                            bufferedReader.readLine();  
                        }
                        lineTxt = bufferedReader.readLine();
                    }
                //pstmt.executeBatch();
                //pstmt.clearBatch();     
                // 展开结果集数据库
                conn.commit();
                cos_time = cos_time + System.currentTimeMillis() - start_time; 
                count++;
            }
                System.out.println("线程id"+num+"执行了"+count+"次");
                System.out.println("线程id"+num+"执行了"+cos_time/count+"ms");
                pstmt.close();
                //pstmt1.close();
                conn.close();
            }catch(SQLException se){
                // 处理 JDBC 错误
                se.printStackTrace();
            }catch(Exception e){
                // 处理 Class.forName 错误
                e.printStackTrace();
            }finally{
                // 关闭资源
                try{
                    if(stmt!=null) stmt.close();
                }catch(SQLException se2){
                }// 什么都不做
                try{
                    if(conn!=null) conn.close();
                }catch(SQLException se){
                    se.printStackTrace();
                }
            }
            System.out.println("Goodbye!");
            return 0;
        }

        public static BufferedReader readTxtFile(String filePath){

            try {
            
            String encoding="utf-8";
            
            File file=new File(filePath);
            
            if(file.isFile() && file.exists()){ //判断文件是否存在
            
            InputStreamReader read = new InputStreamReader(
            
            new FileInputStream(file),encoding);//考虑到编码格式
            
            BufferedReader bufferedReader = new BufferedReader(read);
            
            //String lineTxt = null;
            
            // while((lineTxt = bufferedReader.readLine()) != null){
            
            // System.out.println(lineTxt);
            
            // }
            
            // read.close();
            return bufferedReader;
            
            }else{
            
            System.out.println("找不到指定的文件");
            
            }
            
            } catch (Exception e) {
            
            System.out.println("读取文件内容出错");
            
            e.printStackTrace();
            
            }
            return null;
            
            }

   }
}

