package HBase;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.zip.CRC32;

public class HBaseTest {
    private Connection connection;
    private Admin admin;


    Random r = new Random();

    private static IPSeekerExt ipSeekerExt = new IPSeekerExt();


    @Before
    public void setUp() throws Exception {
        //取得一个数据库连接的配置参数对象
        Configuration conf = HBaseConfiguration.create();
        //设置连接参数，HBase数据库所在的主机IP
        conf.set("hbase.zookeeper.quorum", "slave1,slave2,slave3");
        //设置连接参数，HBase数据库使用的端口
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        //取得一个数据库连接对象
        connection = ConnectionFactory.createConnection(conf);
        //取得一个数据库元数据操作对象
        admin = connection.getAdmin();
    }
    @Test
    public void createTable() throws IOException {
        System.out.println("---------------CREATE TABLE START----------创建表 开始-------");
        // table name
        String tableNameString = "eventlog";

        // create a new table obj
        TableName tableName = TableName.valueOf(tableNameString);

        if(admin.tableExists(tableName)){

            System.out.println("the table is exits! --- 这个表已经存在");
        }

        else{
            //数据表描述对象
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
            //列族描述对象
            HColumnDescriptor family= new HColumnDescriptor("log");
            // create a new column  在数据表中新建一个列族
            hTableDescriptor.addFamily(family);
            //新建数据表
            admin.createTable((TableDescriptor) hTableDescriptor);
        }
        System.out.println("---------------create table END------创建表 结束-----------");
    }
    @Test
    public void insert() throws IOException{

        System.out.println("---------------insert data START-----插入数据 开始------------");
        //取得一个数据表对象
        Table table = connection.getTable(TableName.valueOf("eventlog"));

/*        //需要插入数据库的数据集合
        List<Put> putList = new ArrayList<Put>();
        Put put;
        //生成数据集合
        for(int i = 0; i < 1; i++){
            put = new Put(Bytes.toBytes("row" + i));
            put.addColumn(Bytes.toBytes("base"), Bytes.toBytes("name"), Bytes.toBytes("carName" + i));
            putList.add(put);
            System.out.println(put);
        }*/

        HBaseTest tDataMaker = new HBaseTest();



        // 用户标示u_ud  随机生成8位
        String uuid = String.format("%08d", r.nextInt(99999999));
        // 会员标示u_mid  随机生成8位
        String memberId = String.format("%08d", r.nextInt(99999999));

        List<Put> puts = new ArrayList<Put>();
        for (int i = 0; i < 100; i++) {

            if(i%5==0) {
                uuid = String.format("%08d", r.nextInt(99999999));
                memberId = String.format("%08d", r.nextInt(99999999));
            }
            if(i%6==0) {
                uuid = String.format("%08d", r.nextInt(99999999));
                memberId = String.format("%08d", r.nextInt(99999999));
            }

            Date d = tDataMaker.getDate("20211212");

            String serverTime = ""+d.getTime();

            Put put = tDataMaker.putMaker(uuid, memberId, serverTime);
            puts.add(put);
        }

//        System.out.println(putList);
        //将数据集合插入到数据库
        table.put(puts);
        System.out.println(puts);

        System.out.println("---------------insert data END------插入数据 结束-----------");
    }
//    @Test
//    public void updateTable() throws IOException{
//
//        System.out.println("---------------update START------更新表数据 开始-----------");
//        //取得一个数据表对象
//        Table table = connection.getTable(TableName.valueOf("t_car"));
//
//        Put put = new Put(Bytes.toBytes("row" + 6));
//        put.addColumn(Bytes.toBytes("base"), Bytes.toBytes("name"), Bytes.toBytes("car_name"));
//        table.put(put);
//
//        System.out.println("---------------update End--------更新表数据 结束---------");
//    }
    @Test
    public void deleteTable() throws IOException{

        System.out.println("---------------delete table START------删除表 开始-----------");
        //设置表状态为无效
        admin.disableTable(TableName.valueOf("t_car"));
        //删除指定的数据表
        admin.deleteTable(TableName.valueOf("t_car"));

        System.out.println("---------------delete table End--------删除表 结束---------");
    }

    /**
     * select whole table data
     */
    @Test
    public void queryTable() throws IOException{
        System.out.println("---------------select whole table data START-----查询整张表 开始------------");
        //get table object
        Table table = connection.getTable(TableName.valueOf("t_car"));
        // get whole table data
        ResultScanner scanner = table.getScanner(new Scan());
        // print data
        for (Result result : scanner) {
            byte[] row = result.getRow();
            System.out.println("row key is:" + new String(row));

            List<Cell> listCells = result.listCells();
            for (Cell cell : listCells) {
                byte[] familyArray = cell.getFamilyArray();
                byte[] qualifierArray = cell.getQualifierArray();
                byte[] valueArray = cell.getValueArray();
                System.out.println("row value is:" + new String(familyArray) + new String(qualifierArray)
                        + new String(valueArray));
            }
        }
        System.out.println("---------------select whole talbe data  END-------查询整张表 结束----------");
    }

    /**
     * select data by rowkey
     */
    @Test
    public void queryTableByRowKey() throws IOException{
        System.out.println("---------------select data by rowkey START-----------------");
        //取得数据表对象
        Table table = connection.getTable(TableName.valueOf("t_car"));
        Get get = new Get("row6".getBytes());
        Result result = table.get(get);

        byte[] row = result.getRow();
        System.out.println("row key is:" + new String(row));

        List<Cell> listCells = result.listCells();
        for (Cell cell : listCells) {

            byte[] familyArray = cell.getFamilyArray();
            byte[] qualifierArray = cell.getQualifierArray();
            byte[] valueArray = cell.getValueArray();

            System.out.println("family:" +new String(familyArray));
            System.out.println("qualifier:" + Bytes.toString(qualifierArray) );
            System.out.println("value:" + Bytes.toString(valueArray) );
        }
        System.out.println("---------------select data by rowkey END-----------------");

    }
    /**
     * select data by conditions
     */
//    @Test
//    public void queryTableByCondition() throws IOException{
//        System.out.println("---------------select data by conditions START----按条件查询表数据 开始-------------");
//        //取得数据表对象
//        Table table = connection.getTable(TableName.valueOf("t_car"));
//
//        // create a filter（创建一个查询过滤器）
//        Filter filter = new SingleColumnValueFilter(Bytes.toBytes("base"), Bytes.toBytes("name"),
//                CompareOperator.EQUAL, Bytes.toBytes("carName6"));
//        //创建一个数据表扫描器
//        Scan scan = new Scan();
//        //将查询过滤器加入到数据表扫描器对象
//        scan.setFilter(filter);
//        //执行查询操作，并取得查询结果
//        ResultScanner scanner = table.getScanner(scan);
//        //循环输出查询结果
//        for (Result result : scanner) {
//            byte[] row = result.getRow();
//            System.out.println("row key is:" + new String(row));
//            List<Cell> listCells = result.listCells();
//            for (Cell cell : listCells) {
//                byte[] familyArray = cell.getFamilyArray();
//                byte[] qualifierArray = cell.getQualifierArray();
//                byte[] valueArray = cell.getValueArray();
//
//                System.out.println("family:" +Bytes.toString(familyArray));
//                System.out.println("qualifier:" + Bytes.toString(qualifierArray) );
//                System.out.println("value:" + Bytes.toString(valueArray) );
//            }
//        }
//        System.out.println("---------------select data by conditions END------按条件查询表数据 结束-----------");
//    }
    /**
     * truncate table 删除表中所有行 清空表
     */
    @Test
    public void truncateTable() throws IOException{

        System.out.println("--------------truncate table START-----------------");
        TableName tableName = TableName.valueOf("t_car");
        admin.disableTable(tableName);
        admin.truncateTable(tableName, true);
        System.out.println("---------------truncate table end----------------");
    }
    /**
     *delete row 删除行（键）
     */
    @Test
    public void deleteByRowKey() throws IOException{

        System.out.println("---------------delete row START-----------------");
        Table table = connection.getTable(TableName.valueOf("t_car"));
        Delete delete = new Delete(Bytes.toBytes("row2"));
        table.delete(delete);
        System.out.println("---------------delete row End-----------------");

    }


    SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
    /**
     * 随机生成时间
     * @param str  年月日 20160101
     * @return
     */
    public Date getDate(String str) {
        str = str + String.format("%02d%02d%02d", new Object[]{r.nextInt(24), r.nextInt(60), r.nextInt(60)});
        Date d = new Date();
        try {
            d = sdf.parse(str);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return d;
    }

    /**
     * 测试数据
     * day 时间  年月日
     * lognum 日志条数
     */
    public Put putMaker(String uuid, String memberId, String serverTime) {

        Map<String, Put> map = new HashMap<String, Put>();

        byte[] family = Bytes.toBytes(EventLogConstants.EVENT_LOGS_FAMILY_NAME);

        // 解析日志
        Map<String, String> clientInfo = LoggerUtil.handleLog("......");

        clientInfo.put(EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME, serverTime);
        clientInfo.put(EventLogConstants.LOG_COLUMN_NAME_UUID, uuid);
        clientInfo.put(EventLogConstants.LOG_COLUMN_NAME_PLATFORM, "website");

        clientInfo.put(EventLogConstants.LOG_COLUMN_NAME_EVENT_NAME, EventNames[r.nextInt(EventNames.length)]);
        clientInfo.put(EventLogConstants.LOG_COLUMN_NAME_SESSION_ID, SessionIDs[r.nextInt(SessionIDs.length)]);
        clientInfo.put(EventLogConstants.LOG_COLUMN_NAME_CURRENT_URL, CurrentURLs[r.nextInt(CurrentURLs.length)]);


        clientInfo.put(EventLogConstants.LOG_COLUMN_NAME_OS_NAME, this.getOsName());
        clientInfo.put(EventLogConstants.LOG_COLUMN_NAME_OS_VERSION, this.getOsVersion());
        clientInfo.put(EventLogConstants.LOG_COLUMN_NAME_BROWSER_NAME, this.getBrowserName());
        clientInfo.put(EventLogConstants.LOG_COLUMN_NAME_BROWSER_VERSION, this.getBrowserVersion());

        String ip = IPs[r.nextInt(IPs.length)];
        IPSeekerExt.RegionInfo info = ipSeekerExt.analyticIp(ip);
        if (info != null) {
            clientInfo.put(EventLogConstants.LOG_COLUMN_NAME_COUNTRY, info.getCountry());
            clientInfo.put(EventLogConstants.LOG_COLUMN_NAME_PROVINCE, info.getProvince());
            clientInfo.put(EventLogConstants.LOG_COLUMN_NAME_CITY, info.getCity());
        }

        String eventName = EventNames[r.nextInt(EventNames.length)];

        //生成rowkey
        String rowkey = this.generateRowKey(uuid, memberId, eventName, serverTime);

        Put put = new Put(Bytes.toBytes(rowkey));
        for (Map.Entry<String, String> entry : clientInfo.entrySet()) {
            put.addColumn(family, Bytes.toBytes(entry.getKey()), Bytes.toBytes(entry.getValue()));
        }

        return put;
    }


    private String[] CurrentURLs = new String[]{"http://www.jd.com",
            "http://www.tmall.com","http://www.sina.com","http://www.weibo.com"};

    private String[] SessionIDs = new String[]{"1A3B4F83-6357-4A64-8527-F092169746D3",
            "12344F83-6357-4A64-8527-F09216974234","1A3B4F83-6357-4A64-8527-F092169746D8"};

    private String[] IPs = new String[]{"58.42.245.255","39.67.154.255",
            "23.13.191.255","14.197.148.38","14.197.149.137","14.197.201.202","14.197.243.254"};

    private String[] EventNames = new String[]{"e_l","e_pv"};

    private String[] BrowserNames = new String[]{"FireFox","Chrome","aoyou","360"};


    /**
     * 获取随机的浏览器名称
     * @return
     */
    private String getBrowserName() {
        return BrowserNames[r.nextInt(BrowserNames.length)];
    }


    /**
     * 获取随机的浏览器版本信息
     * @return
     */
    private String getBrowserVersion() {
        return (""+r.nextInt(9));
    }

    /**
     * 获取随机的系统版本信息
     * @return
     */
    private String getOsVersion() {
        return (""+r.nextInt(3));
    }

    private String[] OsNames = new String[]{"window","linux","ios"};
    /**
     * 获取随机的系统信息
     * @return
     */
    private String getOsName() {
        return OsNames[r.nextInt(OsNames.length)];
    }

    private CRC32 crc32 = new CRC32();

    /**
     * 根据uuid memberid servertime创建rowkey
     * @param uuid
     * @param memberId
     * @param eventAliasName
     * @param serverTime
     * @return
     */
    private String generateRowKey(String uuid, String memberId, String eventAliasName, String serverTime) {
        StringBuilder sb = new StringBuilder();
        sb.append(serverTime).append("_");
        this.crc32.reset();
        if (StringUtils.isNotBlank(uuid)) {
            this.crc32.update(uuid.getBytes());
        }
        if (StringUtils.isNotBlank(memberId)) {
            this.crc32.update(memberId.getBytes());
        }
        this.crc32.update(eventAliasName.getBytes());
        sb.append(this.crc32.getValue() % 100000000L);
        return sb.toString();
    }

}
