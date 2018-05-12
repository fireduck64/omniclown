import org.bitcoinj.core.Sha256Hash;
import org.bitcoinj.core.Transaction;
import org.bitcoinj.core.TransactionInput;
import org.bitcoinj.core.TransactionOutPoint;
import java.util.Set;
import java.util.Collection;
import java.util.HashSet;
import java.util.HashMap;
import java.util.TreeMap;
import com.google.common.collect.Multimap;
import com.google.common.collect.HashMultimap;
import com.google.protobuf.ByteString;

public class MemPooler extends Thread
{
  private OmniClown clown;

  private MemPoolInfo latest_info;
  private BitcoinRPC bitcoin_rpc;
  private Broadcaster my_broadcaster;
  private String name;


  public MemPooler(OmniClown clown, BitcoinRPC bitcoin_rpc, Broadcaster my_broadcaster)
  {
    this.clown = clown;
    this.bitcoin_rpc = bitcoin_rpc;
    this.my_broadcaster = my_broadcaster;
    setName("MemPooler");
    setDaemon(false);

    this.name = bitcoin_rpc.getName();

  }

  public void run()
  {
    while(true)
    {
      try
      {
         runInner();
      }
      catch(Throwable t)
      {
        clown.getEventLog().alarm("Error in MemPooler:"+name+" " + t.toString());
        clown.getEventLog().logTrace(t);
      }
      try
      {
        Thread.sleep(15000L);
      }
      catch(Throwable t)
      {
        throw new RuntimeException(t);
      }

    }
  }

  public void triggerUpdate()
    throws Exception
  {
    //TODO - implement
  }
  private void runInner()
    throws Exception
  {
    HashSet<Sha256Hash> new_tx_set = new HashSet<>();

    new_tx_set.addAll(bitcoin_rpc.getMempoolList());

    my_broadcaster.addKnown(new_tx_set);

    MemPoolInfo prev_info = latest_info;

    MemPoolInfo new_info = new MemPoolInfo();

    new_info.tx_set.addAll(new_tx_set);
    int existing_tx=0;
    int new_tx=0;
    int fail_tx=0;

    HashSet<Sha256Hash> new_tx_list = new HashSet<>();

    HashMap<Sha256Hash, ByteString> broadcast_map = new HashMap<>();

    for(Sha256Hash tx_hash : new_tx_set)
    {
      ByteString tx_data = null;

      if (prev_info != null)
      {
        tx_data = prev_info.tx_data_map.get(tx_hash);
        existing_tx++;
      }
      if (tx_data == null)
      {
        tx_data = bitcoin_rpc.getTransaction(tx_hash);
        if (tx_data != null)
        {
          new_tx++;
          new_tx_list.add(tx_hash);
          broadcast_map.put(tx_hash, tx_data);
        }
        else
        {
          clown.getEventLog().log(String.format("MemPooler_%s: Failed to load TX - %s", name, tx_hash.toString()));
        }
      }

      if (tx_data == null)
      {
        fail_tx++;
      }
      else
      {
        new_info.tx_data_map.put(tx_hash, tx_data);
      }
    }

    clown.getEventLog().log(String.format("Mempool_%s size: %d (existing %d, new %d, fail %d) %s", name, new_tx_set.size(), existing_tx, new_tx, fail_tx, getUniqueReport(new_tx_set)));

    latest_info = new_info;

    clown.broadcastAll(broadcast_map);

  }

  public String getUniqueReport(HashSet<Sha256Hash> tx_set)
  {
    TreeMap<Integer, Integer> count_map = new TreeMap<>();
    for(Sha256Hash tx : tx_set)
    {
      int n = clown.countLocal(tx);
      if (!count_map.containsKey(n)) count_map.put(n, 0);
      count_map.put(n, count_map.get(n) + 1);
    }

    StringBuilder sb=new StringBuilder();
    sb.append("{");

    boolean first=true;
    for(int n : count_map.keySet())
    {
      if (!first) sb.append(",");
      sb.append(n);
      sb.append(":");
      sb.append(count_map.get(n));

      first=false;
    }
    sb.append("}");


    return sb.toString();

  }


  public class MemPoolInfo
  {
    HashSet<Sha256Hash> tx_set;
    HashMap<Sha256Hash, ByteString> tx_data_map;

    public MemPoolInfo()
    {
      tx_set = new HashSet<>(256, 0.5f);
      tx_data_map = new HashMap<>(256, 0.5f);
    }

  }

}
