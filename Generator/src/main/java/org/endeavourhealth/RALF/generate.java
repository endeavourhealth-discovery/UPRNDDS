package org.endeavourhealth.RALF;

import java.io.*;
import java.util.*;

import org.endeavourhealth.RALF.repository.Repository;
import OpenPseudonymiser.Crypto;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import javax.net.ssl.HttpsURLConnection;
import java.io.File;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;

import org.json.*;

public class generate implements AutoCloseable {

    private final Repository repository;

    public generate(final Properties properties) throws Exception {
        this(properties, new Repository(properties));
    }

    public generate(final Properties properties, final Repository repository) {
        this.repository = repository;
    }

    public String getToken()
    {
        String token="";
        try {
            String encoded = "password="+repository.tokenpass+"&username="+repository.tokenusername+"&client_id="+repository.tokenclientid+"&grant_type="+repository.tokengrant;
            URL obj = new URL(repository.tokenurl);
            HttpsURLConnection con = (HttpsURLConnection) obj.openConnection();

            con.setRequestMethod("POST");
            con.setRequestProperty("Content-Type","application/x-www-form-urlencoded");

            con.setDoOutput(true);

            BufferedOutputStream bos = new BufferedOutputStream(con.getOutputStream());

            BufferedInputStream bis = new BufferedInputStream(new StringBufferInputStream(encoded));

            int i;
            // read byte by byte until end of stream
            while ((i = bis.read()) > 0) {
                bos.write(i);
            }
            bis.close();
            bos.close();

            System.out.println(con.getResponseMessage());

            InputStream inputStream;
            int responseCode = con.getResponseCode();

            String response = "";

            if ((responseCode >= 200) && (responseCode <= 202)) {
                inputStream = con.getInputStream();
                System.out.println(con.getHeaderField("location"));
                int j;
                while ((j = inputStream.read()) > 0) {
                    //System.out.print((char) j);
                    response = response + (char)j;
                }

            } else {
                inputStream = con.getErrorStream();
            }
            con.disconnect();

            System.out.println(response.toString());

            JSONObject json = new JSONObject(response.toString());
            System.out.println(json.getString("access_token"));

            token = json.getString("access_token");


        } catch (Exception e) {
            e.printStackTrace();
        }

        return token;
    }

    public String PostAddresses(String addresses)
    {
        URLConnection urlconnection = null;
        String response="";
        try {

            String url= repository.baseurl+ "postadr";

            URL obj = new URL(url);
            HttpsURLConnection con = (HttpsURLConnection) obj.openConnection();

            con.setDoOutput(true);
            con.setRequestMethod("POST");

            con.setRequestProperty("Authorization","Bearer "+repository.token);

            BufferedOutputStream bos = new BufferedOutputStream(con.getOutputStream());

            BufferedInputStream bis = new BufferedInputStream(new StringBufferInputStream(addresses));

            int i;
            // read byte by byte until end of stream
            while ((i = bis.read()) > 0) {
                bos.write(i);
            }
            bis.close();
            bos.close();

            System.out.println(con.getResponseMessage());

            InputStream inputStream;
            int responseCode = con.getResponseCode();

            if ((responseCode >= 200) && (responseCode <= 202)) {
                inputStream = con.getInputStream();
                System.out.println(con.getHeaderField("location"));
                int j;
                while ((j = inputStream.read()) > 0) {
                    //System.out.print((char) j);
                    response = response + (char)j;
                }

            } else {
                inputStream = con.getErrorStream();
            }
            con.disconnect();

        } catch (IOException e) {
            e.printStackTrace();
        }
        return response;
    }

    public String getPage(String page, String uuid)
    {
        String response = "";

        String url= repository.baseurl+"getadr?page="+page+"&uuid="+uuid;

        try {
            URL obj = new URL(url);

            HttpsURLConnection con = (HttpsURLConnection) obj.openConnection();

            con.setRequestMethod("GET");
            con.setRequestProperty("Authorization","Bearer "+repository.token);

            int responseCode = con.getResponseCode();

            String output;
            BufferedReader in = new BufferedReader(
                    new InputStreamReader(con.getInputStream()));

            while ((output = in.readLine()) != null) {
                response = response + output;
            }
            in.close();


        } catch (IOException e) {
                e.printStackTrace();
        }
        return response;
    }

    public void export() throws Exception {

        repository.token = getToken();

        UUID uuid = UUID.randomUUID();
        String uuidStr = uuid.toString();

        // repository.lastid = "0";

        if (repository.CheckAPILog().equals("1")) {
            System.out.print("API error: unable to continue");
            return;
        }

        repository.lastid = repository.GetLastIdProcessed();
        if (repository.lastid.length()==0) {
            repository.lastid="0";
        }

        // move 200 records on
        Integer stop = Integer.parseInt(repository.OnwardLastId("200"));

        // calculate the end of the bulk load
        // repository.CalcEndOfBulk();

        Integer q = 0;
        String output = ""; String mid = "";

        List<List<String>> addguids = new ArrayList<>();

        // test to see if the bulk has been done?
        while (true) {
            List<List<String>> addresses = repository.GetPage();
            if (addresses.isEmpty()) break;
            if (Integer.parseInt(repository.lastid)>stop) break;

            // add1,add2,add3,add4,city,postcode,org,nor,id
            output="";
            for (List<String> rec : addresses) {
                output = output+rec.get(0)+"~"+rec.get(1)+"~"+rec.get(2)+"~"+rec.get(3)+"~"+rec.get(4)+"~"+rec.get(5)+"~"+rec.get(6)+"~"+rec.get(8)+"~"+uuidStr+"~"+rec.get(7)+"`";
                q =q +1;
            }
            System.out.println(output);
            mid = PostAddresses(output);

            if (!mid.equals("ok")) {
                System.out.println(mid);
                output = output.replace("'","");
                repository.LogAPIError(output);
                break;
            }

            //List<String> row = new ArrayList<>();
            //row.add(mid);

        }

        //if (!mid.equals("ok"))
        //{
        //    System.out.println("Something went wrong!");
        //    return;
        //}

        Integer page=1; String org=""; String adrid=""; String uprn=""; String z=""; String query="";
        String nhsnumber=""; String nor="";

        Crypto crypto = new Crypto();

        File encryptedSalt = new File(repository.encyptsaltlocation);
        crypto.SetEncryptedSalt(encryptedSalt);

        TreeMap nameValue = new TreeMap();

        while (true) {
            String response = getPage(page.toString(), uuidStr);
            if (response.length()==0) break;

            String[] ss = response.split("\\`");
            String digest = "0";
            for (int i = 0; i < ss.length; i++) {
                z = ss[i];
                String[] rec = z.split("\\,",-1);
                org = rec[0]; adrid = rec[1]; uprn = rec[2]; nor = rec[3];
                digest="";
                if (!uprn.isEmpty()) {
                    System.out.println(adrid+" * "+uprn);

                    // get the patients nhs number
                    query = "SELECT distinct p.nhs_number FROM "+repository.dbschema+".patient_address adr JOIN "+repository.dbschema+".patient p on p.id = adr.patient_id WHERE adr.id="+adrid;

                    nhsnumber = repository.GetNHSNumber(query);

                    nameValue.put("NHSNumber", nhsnumber);
                    nameValue.put("UPRN", uprn);

                    digest = crypto.GetDigest(nameValue);
                    System.out.println(digest); // <= RALF

                    repository.InsertUPRNTable(nor,org,"0",uprn, digest);
                }

                repository.UpdateLastId(adrid);
            }
            page = page +1;
        }

        /*
        String jsonStr = "{\"name\": \"i30\", \"brand\": \"Hyundai\"}";
        ObjectMapper mapper = new ObjectMapper();
        JsonNode jsonNode = mapper.readTree(jsonStr);
        System.out.println(jsonNode.toString());
        System.out.println(jsonNode.get("name").asText());
        */
    }

    @Override
    public void close() throws Exception {
        repository.close();
    }

}

