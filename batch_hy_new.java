import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import java.io.BufferedReader;

import java.io.File;

import java.io.FileInputStream;

import java.io.InputStreamReader;



public class batch_hy_new {
 
    static String  IP_Port;
    static String user;
    static String passwd;
    static String database;
    static Integer pool_size;
    static Integer bench_size;
    public static String url = "useCursorFetch=true&useServerPrepStmts=true&cachePrepStmts=true&useConfigs=maxPerformance&prepStmtCacheSqlLimit=20480&prepStmtCacheSize=1000"; 
    static class  InitDate {
        public InitDate(long maxCycle,long maxSer){
           this.maxCycle = maxCycle;
           this.maxSer = maxSer;
        }
        public Long maxCycle;
        public Long maxSer;
    };

    public static void main(String[] args) {
        Connection conn = null;
        Statement stmt = null;
        if (read_args(args)) {
            return ;
        }
        String[] IpPort = IP_Port.split(";");
        //CountDownLatch latch = new CountDownLatch(POOL_SIZE);
        InitDate initDate = getMax(IpPort[0]);
        try{
            
            List<Future<ThreadResultInfo>> fList = new ArrayList<>();
            ExecutorService es = Executors.newFixedThreadPool(pool_size);
            for(int i = 0; i < pool_size; ++i) {
                fList.add(es.submit(new con_work(i, pool_size, bench_size, IpPort[i%IpPort.length], initDate)));
            }
            double TPS  = 0;
            for(Future<ThreadResultInfo> f : fList) {
               
                try {
                   ThreadResultInfo threadInfo = f.get();
                   TPS = threadInfo.getCount()*1000*1.0/threadInfo.getCostTime()+TPS;
                   
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            System.out.println("TPS:"+TPS);

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

    public static boolean read_args(String[] args) {
        if (args.length != 6) {
            System.out.println("Usage: java batch_hy <IP:port;...> <user> <passwd> <pool_size> <bench_size> <database>");
            return true;
        }
        IP_Port = args[0];
        user = args[1];
        passwd = args[2];
        pool_size = Integer.parseInt(args[3]);
        bench_size = Integer.parseInt(args[4]);
        database = args[5];
        return false;

    }

    static InitDate getMax(String ipPort) {
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;

        String JDBC_DRIVER = "com.mysql.jdbc.Driver";  

        long maxCycle = 0;
        long maxSer = 0;
        try{
            Class.forName(JDBC_DRIVER);
            conn = DriverManager.getConnection("jdbc:mysql://"+ipPort+"/"+database+"?"+url, user, passwd);
            stmt = conn.createStatement();
            String sql = "select max(setl_cycle) as id from bs_intr_acsetl_detail;";
            rs = stmt.executeQuery(sql);
            while(rs.next()) {
                maxCycle = rs.getLong("id");
            }
            sql = "select max(serial_no) as id from ACCOUNT_DETAIL;";
            rs = stmt.executeQuery(sql);
            while(rs.next()) {
                maxSer = rs.getLong("id");
            }
            
        }catch(Exception se){
            // 处理 JDBC 错误
            se.printStackTrace();
        }finally{
            // 关闭资源
            try{
              stmt.close();
              conn.close();
            }catch(SQLException se){
                se.printStackTrace();
            }
        }
        InitDate initDate = new InitDate(maxCycle, maxSer);
        return initDate;
    }

    static class ThreadResultInfo{
        private int threadNo;
        private long count;
        private long costTime;

        public int getThreadNo(){
            return this.threadNo;
        }

        public void setThreadNo(int thread_No){
            this.threadNo = thread_No;
        }

        public long getCount(){
            return this.count;
        }
        public void setCount(long count){
            this.count = count;
        }

        public long getCostTime(){
            return this.costTime;
        }
        public void setCostTime(long costTime){
            this.costTime=costTime;
        }
    }

    public static class con_work implements Callable<ThreadResultInfo> {

        private int num;
        private int tot;
        private int bench_num;
        private long maxCycle;
        private long maxSer;
        List<String> list ;
        // MySQL 8.0 以下版本 - JDBC 驱动名及数据库 URL
        static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";  
        // MySQL 8.0 以上版本 - JDBC 驱动名及数据库 URL
        //static final String JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";  
        //static final String DB_URL = "jdbc:mysql://localhost:3306/RUNOOB?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC";
        // 注册 JDBC 驱动
        String user_id = "";
        int task_user_id = 1;
        boolean user_end = false;
        String areaCode ;
        int task_id ;
        String IpPort = "";

        public con_work(int num, int tot, int bench_num,String IpPort, InitDate initDate) {
            this.num = num;
            this.tot = tot;
            this.bench_num = bench_num;
            this.IpPort = IpPort;
            this.maxCycle = initDate.maxCycle+1;
            this.maxSer = initDate.maxSer + num + 1;
            task_id = num +100; 


        }
        public String get_info_id() {
            String info_id = "33011"  + areaCode ;
            int num = task_user_id%7000 ;
            for (int j = 0; j < 10- Integer.toString(num).length(); j++) {
                info_id = info_id + "0";
            }
            info_id = info_id + Integer.toString(num);
            return info_id;

        }
        
        void init_status(){
            task_user_id = 1;
            user_end = false;
            areaCode = "10" + Integer.toString(task_id);
        }
        // 数据库的用户名与密码，需要根据自己的设置
        public ThreadResultInfo  call() throws ClassNotFoundException{
            Class.forName(JDBC_DRIVER);
            Connection conn = null;
            Statement stmt = null;
            //负载均衡
            try{
                String DB_URL = null;
                DB_URL = "jdbc:mysql://"+IpPort+"/"+database+"?"+url;
                // 打开链接
                System.out.println("连接数据库...");
                
                conn = DriverManager.getConnection(DB_URL,user, passwd);
                conn.setAutoCommit(false);
                // 执行查询
                System.out.println(" 实例化Statement对象...");
                //执行sql
                String sql_select = "select `area_code` ,`account_no` ,`product_code` ,`gl_class` ,`virtual_flag` ,`real_flag` ,`collect_flag` ,`balance_type` ,`balance_ctrl` ,"
                + "`sync_flag` ,`cur_type` ,`spot_type` ,`close_flag` ,`max_acc_cycle_no` ,`max_clearing_times` ,`dayend_cycle_no` ,`acc_date` = `str_to_date` (acc_date,'%Y-%m-%d %H:%i:%s'),`last_acc_date` = `str_to_date` (last_acc_date,'%Y-%m-%d %H:%i:%s'),"
                + "`open_date` = `str_to_date` (open_date,'%Y-%m-%d %H:%i:%s'),`close_date` = `str_to_date` (close_date,'%Y-%m-%d %H:%i:%s'),`detail_cnt` ,`debit_amt` ,`credit_amt` ,`debit_cnt` ,`credit_cnt` ,`balance` ,`pre_clearing_times` ,"
                + "`pre_times_balance` ,`pre_times_dcnt` ,`pre_times_damt` ,`pre_times_ccnt` ,`pre_times_camt` ,`pre_cycle_no` ,`pre_balance` ,`pre_debit_cnt` ,`pre_credit_cnt` ,"
                + "`pre_debit_amt` ,`pre_credit_amt` ,`dac` from  account_dynamic WHERE `account_no` = ? For update;";
                String sql_update = "UPDATE `account_dynamic` SET credit_amt = credit_amt+1  WHERE `account_no` = ?;";
                String sql_insert1 = "insert into bs_intr_acsetl_detail values(?,now(),?,'4856','s','5842',?,'s',?,'cyctftdrdsr',"
                +" 'scdwewvcd', 'w', 'cnsdubhvwued', 'ncusncd', 'fcd', '1', 578412,now(),'sc','sw',154.24,582.24,694.21,'dwefrvss',54852,'s',now(),now(),now())";
                String sql_insert2 = "insert into account_detail values (?,'scxed','sdees','ssdwq','sdcqw',?,'sqdfqe','sace','swda','651',now(),now(),now(),now(),'54785', "
                +"'55841','548',now(),547.25,547.2,5471.2,254.4,548451,'544842','54753',25417,25142,5574,now(),'2','6','54','2','54762','54775','5584','244212',"
                +"'12cas','25441','15456512','1548','15448','10045484','200caijsxscsd','sfcwdvwed','scevcwedsx','sacecwcvd','1','icaohjcidvcd') ";              
                //创建prepare对象
                PreparedStatement pstmt_select = conn.prepareStatement(sql_select);
                PreparedStatement pstmt_update = conn.prepareStatement(sql_update);
                PreparedStatement pstmt_insert1 = conn.prepareStatement(sql_insert1);
                PreparedStatement pstmt_insert2 = conn.prepareStatement(sql_insert2);

                //Statement statement = conn.createStatement();
               
                long start = System.currentTimeMillis();
                long end = start + 600000 ;
                long count = 0;
                long cos_time = 0;
                init_status();
                

                //循环10分钟
                while(end > System.currentTimeMillis()) {
                    long start_time = System.currentTimeMillis();
                    //控制单个事务中的账号数
                    for (int i = 0; i < bench_num; i++) {
                        user_id = "33010"  + areaCode ;
                        for (int j = 0; j < 10- Integer.toString(task_user_id).length(); j++) {
                            user_id = user_id + "0";
                        }
                        user_id = user_id + Integer.toString(task_user_id);
                        maxSer =  maxSer + tot;
                        task_user_id++;
                        pstmt_select.setString(1, user_id);
                        pstmt_update.setString(1, user_id);
                        pstmt_insert1.setLong(1, maxCycle);
                        pstmt_insert1.setString(2, areaCode);
                        pstmt_insert1.setString(3, user_id);
                        pstmt_insert1.setString(4, get_info_id());
                        pstmt_insert2.setLong(1, maxSer);
                        pstmt_insert2.setString(2, user_id);
                        //执行查询
                        ResultSet res = pstmt_select.executeQuery();
                        if (!res.next()) {
                            user_end = true;
                            break;
                        } 
                        res.close();
                        //更新
                        pstmt_update.executeUpdate();
                        //插入
                        pstmt_insert1.executeUpdate();
                        pstmt_insert2.executeUpdate();

                    }
                conn.commit();
                //记录单个事务执行时间
                cos_time = cos_time + System.currentTimeMillis() - start_time; 
                count++;
                if (user_end) {
                    task_id = task_id + tot;
                    if (task_id > 400)
                       break;
                    areaCode = "10" + Integer.toString(task_id);
                    task_user_id = 1;
                    user_end = false;
                }
                
            }
                System.out.println("线程id"+num+"执行了"+count+"次");
                System.out.println("线程id"+num+"执行了"+cos_time/count+"ms");
                pstmt_select.close();
                pstmt_update.close();
                pstmt_insert1.close();
                pstmt_insert2.close();
                conn.close();
                //返回执行信息
                ThreadResultInfo info = new ThreadResultInfo();
                info.setThreadNo(num);
                info.setCount(count);
                info.setCostTime(cos_time);
                return info;
            }catch(SQLException se){
                // 处理 JDBC 错误
                se.printStackTrace();
            }catch(Exception e){
                // 处理 Class.forName 错误
                e.printStackTrace();
            }finally{
                // 关闭资源
                try{
                    //latch.countDown();
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
            return null;
        }

        

   }
}

