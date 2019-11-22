package org.endeavourhealth.RALF.repository;

import com.mysql.cj.jdbc.MysqlDataSource;
import com.mysql.cj.xdevapi.SqlDataResult;
import org.endeavourhealth.common.config.ConfigManager;

import java.sql.*;
import java.time.LocalDate;
import java.util.*;

public class Repository {

    private MysqlDataSource dataSource;

    private Connection connection;

    public String config;

    public String lastid;
    public String lastidprocessed;

    public String dbschema;

    public String tokenurl;
    public String tokenpass;
    public String tokenusername;
    public String tokenclientid;
    public String tokengrant;

    public String token;
    public String baseurl;
    public String encyptsaltlocation;

    public Repository(Properties properties) throws SQLException {
        init( properties );
    }

    public void CalcEndOfBulk() throws SQLException
    {
        String id=""; String dt ="";

        String q="SELECT * FROM "+dbschema+".patient_address order by id desc limit 1";
        PreparedStatement preparedStmt = connection.prepareStatement(q);
        ResultSet rs = preparedStmt.executeQuery();

        if (rs.next()) {
            id = rs.getString("id");
        }

        q = "select * from "+dbschema+".event_log where record_id = "+id;
        preparedStmt = connection.prepareStatement(q);
        rs = preparedStmt.executeQuery();
        if (rs.next()) {
            dt = rs.getString("dt_change");
        }

        // if dt = null, then it must be a test system (set it to today's date)

        preparedStmt.close();
    }

    public void bulk() throws SQLException {
    }

    public String CheckAPILog() throws SQLException
    {
        String apierr = "0";
        String q="select api_error from data_extracts.uprn_Tracker";
        PreparedStatement p = connection.prepareStatement(q);
        ResultSet rs = p.executeQuery();
        if (rs.next())
        {
            apierr = rs.getString("api_error");
        }
        return apierr;
    }

    public void initAPILog() throws SQLException
    {
        String q="select * from data_extracts.uprn_Tracker";
        PreparedStatement p = connection.prepareStatement(q);
        ResultSet rs = p.executeQuery();

        if (!rs.next()) {
            // insert a blank record
            q = "insert into data_extracts.uprn_Tracker (process_date, api_error) values(now(), false);";
            PreparedStatement preparedStmt = connection.prepareStatement(q);
            preparedStmt.execute();
            preparedStmt.close();
        }

        p.close();
    }

    public void InsertUPRNTable(String nor, String orgid, String personid, String UPRN, String RALF)
    {
        try {
            String q="insert into data_extracts.uprn_patient (patient_id, organization_id, person_id, uprn) ";
            q =q + "values ("+nor+","+orgid+","+personid+","+UPRN+");";
            System.out.println(q);
            PreparedStatement preparedStmt = connection.prepareStatement(q);
            preparedStmt.execute();
            preparedStmt.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public String GetLastIdProcessed() throws SQLException
    {
        String q="select last_id_processed from data_extracts.uprn_Tracker";
        String lastid="";
        PreparedStatement p = connection.prepareStatement(q);
        ResultSet rs = p.executeQuery();
        if (rs.next()) {
            lastid=rs.getString("last_id_processed");
            if (lastid==null) lastid="";
        }
        p.close();
        return lastid;
    }

    public String OnwardLastId(String limit) throws SQLException
    {
        String lastid = "";
        String q = "select id from "+dbschema+".patient_address where id > "+this.lastid+" order by id limit "+limit;

        System.out.println(q);

        PreparedStatement p = connection.prepareStatement(q);
        ResultSet rs = p.executeQuery();
        while (rs.next()) {
            lastid=rs.getString("id");
            if (lastid==null) lastid="";
        }
        return lastid;
    }

    public void UpdateLastId(String lastid) throws SQLException
    {
        String q = "update data_extracts.uprn_Tracker set last_id_processed = '"+lastid+"', process_date=now()";
        PreparedStatement preparedStmt = connection.prepareStatement(q);
        preparedStmt.execute();
        preparedStmt.close();
    }

    public void LogAPIError(String output) throws SQLException
    {
        String q="update data_extracts.uprn_Tracker set api_error = true, output='"+output+"'";
        PreparedStatement preparedStmt = connection.prepareStatement(q);
        preparedStmt.execute();
        preparedStmt.close();
    }

    public void deltas()
    {
        // updates
        // SELECT * FROM nwl_subscriber_pid.event_log where dt_change between '2019-10-30 12:50:02.804' and '2019-11-13 23:59:00' and table_id=20 and change_type=1 order by dt_change desc
        String q="";
    }

    public String getConfig(String config)
    {
        String conStr = ConfigManager.getConfiguration("database",config);
        System.out.println(conStr);
        return conStr;
    }

    public String GetNHSNumber(String sql) throws SQLException {
        String nhsnumber="";
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        ResultSet rs = preparedStatement.executeQuery();
        if (rs.next()) {
            nhsnumber = rs.getString("nhs_number");
        }

        preparedStatement.close();

        return nhsnumber;
    }

    public List<List<String>> GetPage()  throws SQLException {
        List<List<String>> result = new ArrayList<>();

        String q = "SELECT * FROM "+dbschema+".patient_address where id >"+this.lastid+" order by id limit 50";

        System.out.println(q);

        PreparedStatement preparedStatement = connection.prepareStatement(q);
        ResultSet rs = preparedStatement.executeQuery();

        String add1=""; String add2=""; String add3=""; String add4=""; String city=""; String postcode="";
        String orgid=""; String nor=""; String id="";

        while (rs.next()) {
            add1=rs.getString("address_line_1");
            add2=rs.getString("address_line_2");
            add3=rs.getString("address_line_3");
            add4=rs.getString("address_line_4");
            city=rs.getString("city");
            postcode=rs.getString("postcode");
            orgid=rs.getString("organization_id");
            nor=rs.getString("patient_id");
            id = rs.getString("id");

            List<String> row = new ArrayList<>();

            if (add1==null) add1="";
            if (add2==null) add2="";
            if (add3==null) add3="";
            if (add4==null) add4="";
            if (city==null) city="";
            if (postcode==null) postcode="";
            if (orgid==null) orgid="";
            if (nor==null) nor="";

            row.add(add1); row.add(add2); row.add(add3);
            row.add(add4); row.add(city);
            row.add(postcode); row.add(orgid); row.add(nor);
            row.add(id);

            result.add(row);

            this.lastid = id;
        }

        preparedStatement.close();

        return result;
    }

    private void init(Properties props) throws SQLException {

        try {
            System.out.println("initializing properties");

            String conStr = getConfig("uprn");
            String[] ss = conStr.split("\\`",-1);

            String mysqlurl = ss[0];
            String mysqlusername = ss[1];
            String mysqlpass = ss[2];
            tokenurl = ss[3];
            String tokenpucg = ss[4]; // password~username~client_id~grant_type

            String[] token = tokenpucg.split("\\~",-1);
            tokenpass = token[0]; tokenusername = token[1];
            tokenclientid = token[2]; tokengrant = token[3];

            dbschema = ss[5];
            baseurl = ss[6];
            encyptsaltlocation = ss[7];

            System.out.println("dbschema: "+dbschema);
            System.out.println("mysqlurl: "+mysqlurl);
            System.out.println("baseurl: "+baseurl);
            System.out.println("salt: "+encyptsaltlocation);

            Scanner scan = new Scanner(System.in);
            System.out.print("Press any key to continue . . . ");
            scan.nextLine();

            dataSource = new MysqlDataSource();

            dataSource.setURL(mysqlurl);
            dataSource.setUser(mysqlusername);
            dataSource.setPassword(mysqlpass);

            //dataSource.setReadOnlyPropagatesToServer(true);

            connection = dataSource.getConnection();

            // bulk();
            initAPILog();
        }
        catch(Exception e)
        {
            System.out.println(e);
        }
    }


    public void close() throws SQLException {
        connection.close();
    }
}
