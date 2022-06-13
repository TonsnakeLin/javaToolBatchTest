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

public class batch_hy {
 
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
                String sql_select = "select `area_code` ,`account_no` ,`product_code` ,`gl_class` ,`virtual_flag` ,`real_flag` ,`collect_flag` ,`balance_type` ,`balance_ctrl` ,"
                + "`sync_flag` ,`cur_type` ,`spot_type` ,`close_flag` ,`max_acc_cycle_no` ,`max_clearing_times` ,`dayend_cycle_no` ,`acc_date` = `str_to_date` (acc_date,'%Y-%m-%d %H:%i:%s'),`last_acc_date` = `str_to_date` (last_acc_date,'%Y-%m-%d %H:%i:%s'),"
                + "`open_date` = `str_to_date` (open_date,'%Y-%m-%d %H:%i:%s'),`close_date` = `str_to_date` (close_date,'%Y-%m-%d %H:%i:%s'),`detail_cnt` ,`debit_amt` ,`credit_amt` ,`debit_cnt` ,`credit_cnt` ,`balance` ,`pre_clearing_times` ,"
                + "`pre_times_balance` ,`pre_times_dcnt` ,`pre_times_damt` ,`pre_times_ccnt` ,`pre_times_camt` ,`pre_cycle_no` ,`pre_balance` ,`pre_debit_cnt` ,`pre_credit_cnt` ,"
                + "`pre_debit_amt` ,`pre_credit_amt` ,`dac` from  account_dynamic WHERE `account_no` = ? For update;";
                String sql_update = "UPDATE `account_dynamic` SET area_code = area_code+1, `product_code` = product_code+1, `gl_class` = gl_class+1, `virtual_flag` = virtual_flag + 1,"
                + "`real_flag` = real_flag+1, `collect_flag` = collect_flag+1, `balance_type` = balance_type+1, `balance_ctrl` = balance_ctrl+1,`sync_flag` = sync_flag+1,`cur_type` = cur_type+1,"
                + "`spot_type` = spot_type+1,`close_flag` = close_flag+1,`max_acc_cycle_no` = max_acc_cycle_no+1,`max_clearing_times` = max_clearing_times+1,`dayend_cycle_no` = dayend_cycle_no+1,"
                + "`acc_date` = acc_date+1,`last_acc_date` = last_acc_date+1,`open_date` = open_date+1,`close_date` = close_date+1,`detail_cnt` = detail_cnt+1,`debit_amt` = debit_amt+1,"
                + "`credit_amt` = credit_amt+1,`debit_cnt` = debit_cnt+1,`credit_cnt` = credit_cnt+1,`balance` = balance+1,`pre_clearing_times` = pre_clearing_times+1,`pre_times_balance` = pre_times_balance+1,"
                + "`pre_times_dcnt` = pre_times_dcnt+1,`pre_times_damt` = pre_times_damt+1,`pre_times_ccnt` = pre_times_ccnt+1,`pre_times_camt` = pre_times_camt+1,`pre_cycle_no` = pre_cycle_no+1,"
                + "`pre_balance` = pre_balance+1,`pre_debit_cnt` = pre_debit_cnt+1,`pre_credit_cnt` = pre_credit_cnt+1,`pre_debit_amt` = pre_debit_amt+1,`pre_credit_amt` = pre_credit_amt+1,"
                + "`dac` = dac+1 WHERE `account_no` = ?;";
                String sql_insert1 = "insert into bs_intr_acsetl_detail values(now(),now(),'5488','4856','s','5842',?,'s','efsfdsvvertrgrgf','cyctftdrdsr',"
                +" 'scdwewvcd', 'w', 'cnsdubhvwued', 'ncusncd', 'fcd', '1', 578412,now(),'sc','sw',154.24,582.24,694.21,'dwefrvss',54852,'s',now(),now(),now())";
                String sql_insert2 = "insert into detail_serial values (?,'scxed','sdees','ssdwq','sdcqw',?,'sqdfqe','sace','swda','651',now(),now(),now(),now(),'54785', "
                +"'55841','548',now(),547.25,547.2,5471.2,254.4,548451,'544842','54753',25417,25142,5574,now(),'2','6','54','2','54762','54775','5584','244212',"
                +"'12cas','25441','15456512','2541','15486','154486','10045484','200caijsxscsd','1','icaohjcidvcd' ";

                PreparedStatement pstmt_select = conn.prepareStatement(sql_select);
                PreparedStatement pstmt_update = conn.prepareStatement(sql_update);
                PreparedStatement pstmt_insert1 = conn.prepareStatement(sql_insert1);
                PreparedStatement pstmt_insert2 = conn.prepareStatement(sql_insert2);
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
                        long time1 = System.currentTimeMillis();
                        String bigUnstring = String.valueOf(num) + time1;
                        //System.out.println(strs[0]+",len:"+strs[0].length());
                        //System.out.println(strs[1]+",len:"+strs[1].length());
                        pstmt_select.setString(1, strs[0]);
                        pstmt_update.setString(1, strs[0]);
                        pstmt_insert1.setString(1, strs[0]);
                        pstmt_insert2.setLong(1, Long.parseLong(bigUnstring));
                        pstmt_insert2.setString(2, strs[0]);
                        java.util.Date date=new java.util.Date();
                        pstmt_select.execute();
                        pstmt_update.executeUpdate();
                        pstmt_insert1.executeUpdate();
                        pstmt_insert2.executeUpdate();
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
                pstmt_select.close();
                pstmt_update.close();
                pstmt_insert1.close();
                pstmt_insert2.close();
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

