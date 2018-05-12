
import java.net.URL;
import java.util.Scanner;
import java.util.List;
import java.util.LinkedList;
import java.net.HttpURLConnection;
import java.io.OutputStream;
import org.apache.commons.codec.binary.Base64;

import org.json.JSONObject;
import org.json.JSONArray;
import org.apache.commons.codec.binary.Hex;
import org.bitcoinj.core.Block;
import org.bitcoinj.core.Sha256Hash;
import java.util.Random;
import com.google.protobuf.ByteString;

public class BitcoinRPC
{
    private String username;
    private String password;
    private String host;
    private int port;
    private EventLog event_log;
    private String name;

    public BitcoinRPC(String name, Config config, EventLog event_log)
    {
        this.name = name;
        config.require(name + "_bitcoind_username");
        config.require(name + "_bitcoind_password");
        config.require(name + "_bitcoind_host");
        config.require(name + "_bitcoind_port");

        username=config.get(name + "_bitcoind_username");
        password=config.get(name + "_bitcoind_password");
        host=config.get(name + "_bitcoind_host");
        port=config.getInt(name + "_bitcoind_port");

        this.event_log = event_log;

    }

    public String getName(){return name;}


    private String getUrl()
    {
        return "http://" + host + ":" + port + "/";
    }

    private String getUrlCommand(String cmd)
    {
        return getUrl();
    }

    public JSONObject sendPost(JSONObject post)
        throws java.io.IOException, org.json.JSONException
    {
        String str = sendPost(getUrl(), post.toString());
        //System.out.println("Returned data: " + str);
        try
        {
          return new JSONObject(str);
        }
        catch(org.json.JSONException t)
        {
          System.out.println("Parse error: " + str);
          throw t;
        }
    }

    protected String sendPost(String url, String postdata)
        throws java.io.IOException
    {
          URL u = new URL(url);

          HttpURLConnection connection = (HttpURLConnection) u.openConnection(); 

          String basic = username+":"+password;
          String encoded = Base64.encodeBase64String(basic.getBytes()); 
          connection.setRequestProperty("Authorization", "Basic "+encoded);
          connection.setDoOutput(true);
          connection.setDoInput(true);
          connection.setInstanceFollowRedirects(false); 
          connection.setRequestMethod("POST"); 
          connection.setRequestProperty("charset", "utf-8");
          connection.setRequestProperty("Content-Length", "" + Integer.toString(postdata.getBytes().length));
          connection.setUseCaches (false);

          OutputStream wr = connection.getOutputStream ();
          wr.write(postdata.getBytes());
          wr.flush();
          wr.close();

          Scanner scan;

          if (connection.getResponseCode() != 500)
          {
              scan = new Scanner(connection.getInputStream());
          } else {
              scan = new Scanner(connection.getErrorStream());
          }

          StringBuilder sb = new StringBuilder();

          while(scan.hasNextLine())
          {
              String line = scan.nextLine();
              sb.append(line);
              sb.append('\n');
          }


          scan.close();
          connection.disconnect();
          return sb.toString();
    }

    public static String getSimplePostRequest(String cmd)
    {
        return "{\"method\":\""+cmd+"\",\"params\":[],\"id\":1}\n";
    }

    public JSONObject doSimplePostRequest(String cmd)
        throws java.io.IOException, org.json.JSONException
    {
        return sendPost(new JSONObject(getSimplePostRequest(cmd)));
    }

    public JSONObject submitTransaction(String transaction_hex)
        throws java.io.IOException, org.json.JSONException
    {
        Random rnd = new Random();
        JSONObject msg = new JSONObject();
        msg.put("id", "" + rnd.nextInt());
        msg.put("method","sendrawtransaction");
        JSONArray params = new JSONArray();
        params.put(transaction_hex);
        msg.put("params", params);
        return sendPost(msg);

    }
    public JSONObject getinfo()
        throws java.io.IOException, org.json.JSONException
    {
        Random rnd = new Random();
        JSONObject msg = new JSONObject();
        msg.put("id", "" + rnd.nextInt());
        msg.put("method","getinfo");
        return sendPost(msg);

    }
    public JSONObject getblockcount()
        throws java.io.IOException, org.json.JSONException
    {
        Random rnd = new Random();
        JSONObject msg = new JSONObject();
        msg.put("id", "" + rnd.nextInt());
        msg.put("method","getblockcount");
        return sendPost(msg);

    }


    public double getRelayFee()
      throws java.io.IOException, org.json.JSONException
    {
      JSONObject info = getinfo();
      JSONObject result = info.getJSONObject("result");
      return result.getDouble("relayfee");
    }

    public int getBlockHeight()
        throws java.io.IOException, org.json.JSONException
    {
        Random rnd = new Random();
        JSONObject msg = new JSONObject();
        msg.put("id", "" + rnd.nextInt());
        msg.put("method","getblockcount");
        JSONObject reply = sendPost(msg);

        return reply.getInt("result");


    }
    public Sha256Hash getBlockHash(int height)
        throws java.io.IOException, org.json.JSONException
    {
      
        Random rnd = new Random();
        JSONObject msg = new JSONObject();
        msg.put("id", "" + rnd.nextInt());
        msg.put("method","getblockhash");
        JSONArray params = new JSONArray();
        params.put(height);
        msg.put("params", params);
        JSONObject reply = sendPost(msg);

        return new Sha256Hash(reply.getString("result"));

    }

    public double getFeeEstimate(int blocks)
        throws java.io.IOException, org.json.JSONException
    {
        Random rnd = new Random();
        JSONObject msg = new JSONObject();
        msg.put("id", "" + rnd.nextInt());
        msg.put("method","estimatefee");
        JSONArray params = new JSONArray();
        params.put(blocks);
        msg.put("params", params);
        JSONObject reply= sendPost(msg);

        return reply.getDouble("result");

    }

    public List<Sha256Hash> getMempoolList()
        throws java.io.IOException, org.json.JSONException
    {
      Random rnd = new Random();
      JSONObject msg = new JSONObject();
      msg.put("id", "" + rnd.nextInt());
      msg.put("method","getrawmempool");
      JSONArray params = new JSONArray();
      msg.put("params", params);
      JSONObject reply= sendPost(msg);
      JSONArray result = reply.getJSONArray("result");

      List<Sha256Hash> tx_list = new LinkedList<Sha256Hash>();
      for(int i=0; i<result.length(); i++)
      {
        String tx_str = result.getString(i);
        Sha256Hash tx_hash = new Sha256Hash(tx_str);
        tx_list.add(tx_hash);
      }

      return tx_list;


    }

    public ByteString getTransaction(Sha256Hash hash)
    {
      while(true)
      {
        try
        {
          Random rnd = new Random();
          JSONObject msg = new JSONObject();
          msg.put("id", "" + rnd.nextInt());
          msg.put("method","getrawtransaction");
          JSONArray params = new JSONArray();
          params.put(hash.toString());
          msg.put("params", params);
          JSONObject reply= sendPost(msg);

          if (reply.isNull("result")) return null;

          String str = reply.getString("result");
          byte[] data = Hex.decodeHex(str.toCharArray());
          return ByteString.copyFrom(data);
        }
        catch(Throwable t)
        {
          event_log.alarm("RPC error on tx "+hash+ " " + t.toString()); 
          t.printStackTrace();
          try { Thread.sleep(5000); } catch(Throwable t2){}
        }
      }
    }
    public JSONObject getVerboseTransaction(Sha256Hash hash)
    {
      while(true)
      {
        try
        {
          Random rnd = new Random();
          JSONObject msg = new JSONObject();
          msg.put("id", "" + rnd.nextInt());
          msg.put("method","getrawtransaction");
          JSONArray params = new JSONArray();
          params.put(hash.toString());
          params.put(1);
          msg.put("params", params);
          JSONObject reply= sendPost(msg);

          if (reply.isNull("result")) return null;

          return reply.getJSONObject("result");
        }
        catch(Throwable t)
        {
          event_log.alarm("RPC error on tx "+hash+ " " + t.toString()); 
          try { Thread.sleep(5000); } catch(Throwable t2){}
        }
      }
    }

    public Sha256Hash getTransactionConfirmationBlock(Sha256Hash hash)
    {
      JSONObject data = getVerboseTransaction(hash);
      if (data == null) return null;

      if (!data.has("blockhash")) return null;

      String blockhash = data.optString("blockhash");
      return new Sha256Hash(blockhash);
    }
 
    public SerializedBlock getBlock(Sha256Hash hash)
    {
      if (hash==null) throw new RuntimeException("WTF");
      while(true)
      {
        try
        {
          Random rnd = new Random();
          JSONObject msg = new JSONObject();
          msg.put("id", "" + rnd.nextInt());
          msg.put("method","getblock");
          JSONArray params = new JSONArray();
          params.put(hash.toString());
          params.put(false);
          //params.put(0);
          msg.put("params", params);
          JSONObject reply= sendPost(msg);

          if (reply.isNull("result")) return null;

          String str = reply.getString("result");
          byte[] data = Hex.decodeHex(str.toCharArray());
          return new SerializedBlock(data);
        }
        catch(Throwable t)
        {
          event_log.alarm("RPC error on block "+hash+ " " + t.toString()); 
          try { Thread.sleep(5000); } catch(Throwable t2){}
        }
      }
    }



    public void testConnection()
        throws java.io.IOException, org.json.JSONException
    {
        try
        {
            System.out.println(getblockcount());
        }
        catch(Throwable t)
        {
            System.out.println("bitcoind getblockcount failed - check bitcoind config items");
            t.printStackTrace();
        }
    }
 
    public JSONObject submitBlock(Block blk)
        throws java.io.IOException, org.json.JSONException
    {
        Random rnd = new Random();

        JSONObject msg = new JSONObject();
        msg.put("method", "submitblock");
        msg.put("id", "" + rnd.nextInt());
        
        JSONArray params = new JSONArray();
        params.put(Hex.encodeHexString(blk.bitcoinSerialize()));
        msg.put("params", params);
        //System.out.println(msg.toString(2));
        return sendPost(msg);
    }



}
