
import org.apache.commons.codec.binary.Hex;
import java.util.HashSet;
import java.util.Map;
import java.util.Collection;
import org.bitcoinj.core.Sha256Hash;
import com.google.protobuf.ByteString;
import org.json.JSONObject;
import org.json.JSONArray;



public class Broadcaster
{
  private BitcoinRPC bitcoin_rpc;
  private OmniClown clown;
  private String name;

  LRUCache<Sha256Hash, Boolean> known_tx;
  LRUCache<Sha256Hash, Boolean> local_tx;

  public Broadcaster(OmniClown clown, BitcoinRPC bitcoin_rpc)
  {
    this.clown = clown;
    this.bitcoin_rpc = bitcoin_rpc;
    this.name = bitcoin_rpc.getName();

    known_tx = new LRUCache<>(256000);
    local_tx = new LRUCache<>(256000);
  }

  public void addKnown(Collection<Sha256Hash> set)
  {
    synchronized(known_tx)
    {
      for(Sha256Hash tx : set)
      {
        known_tx.put(tx, true);
      }
    }
    synchronized(local_tx)
    {
      for(Sha256Hash tx : set)
      {
        local_tx.put(tx, true);
      } 
    }
  }

  public boolean hasLocal(Sha256Hash tx)
  {
    synchronized(local_tx)
    {
      return local_tx.containsKey(tx);
    }
  }

  public void broadcastDirect(Map<Sha256Hash, ByteString> tx_map)
  {
    int to_send_sz = tx_map.size();
    int accepted=0;
    int rejected=0;

    for(Map.Entry<Sha256Hash, ByteString> me : tx_map.entrySet())
    {
      Sha256Hash tx_hash = me.getKey();

      ByteString tx_data = me.getValue();

      try
      {
        JSONObject resp = bitcoin_rpc.submitTransaction( new String(new Hex().encode(tx_data.toByteArray())));
        if (tx_hash.toString().equals(resp.optString("result")))
        {
          accepted++;
        }
        else
        {
          rejected++;
        }
      }
      catch(Throwable t)
      {
        clown.getEventLog().log(t);
      }
    }
    clown.getEventLog().log(String.format(
      "BroadcasterDirect-%s: Input %s, Accepted %d, Rejected %d", 
      name,
      to_send_sz,
      accepted,
      rejected));
  }

  public void broadcast(Map<Sha256Hash, ByteString> tx_map)
  {
    int input_sz = tx_map.size();

    HashSet<Sha256Hash> to_send = new HashSet<>();
    synchronized(known_tx)
    {
      for(Sha256Hash tx_hash : tx_map.keySet())
      {
        if (!known_tx.containsKey(tx_hash)) to_send.add(tx_hash);
      }
    }
    int to_send_sz = to_send.size();
    int accepted=0;
    int rejected=0;

    for(Sha256Hash tx_hash : to_send)
    {
      ByteString tx_data = tx_map.get(tx_hash);
      synchronized(known_tx)
      {
        known_tx.put(tx_hash, true);
      }
      try
      {
        JSONObject resp = bitcoin_rpc.submitTransaction( new String(new Hex().encode(tx_data.toByteArray())));
        if (tx_hash.toString().equals(resp.optString("result")))
        {
          accepted++;
        }
        else
        {
          rejected++;

          /*clown.getEventLog().log(String.format("Broadcaster-%s: TX rejected: %s %s",
            name,
            tx_hash.toString(),
            resp.toString()));*/
            
          
        }
      }
      catch(Throwable t)
      {
        clown.getEventLog().log(t);
      }
    }
    clown.getEventLog().log(String.format(
      "Broadcaster-%s: Input %s, New %d, Accepted %d, Rejected %d", 
      name,
      input_sz,
      to_send_sz,
      accepted,
      rejected));
  }


}
