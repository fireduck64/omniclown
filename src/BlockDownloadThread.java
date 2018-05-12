
import org.bitcoinj.core.Sha256Hash;
import org.bitcoinj.core.Block;
import org.bitcoinj.core.Transaction;

import java.util.HashSet;
import java.util.Map;
import java.util.HashMap;
import com.google.protobuf.ByteString;

public class BlockDownloadThread extends Thread
{
  private OmniClown clown;

  private HashSet<Sha256Hash> downloaded;
  private BitcoinRPC bitcoin_rpc;
  private Broadcaster my_broadcaster;
  private String name;

  private int broadcast_height = 0;

  public BlockDownloadThread(OmniClown clown, BitcoinRPC bitcoin_rpc, Broadcaster my_broadcaster)
  {
    this.clown = clown;
    this.bitcoin_rpc = bitcoin_rpc;
    this.my_broadcaster = my_broadcaster;
    setName("BlockDownloadThread");
    setDaemon(false);

    clown.getConfig().require("blocks_back");

    this.name = bitcoin_rpc.getName();

    downloaded=new HashSet<>();
  }

  public void run()
  {
    while(true)
    {
      try
      {
        runInternal();
      }
      catch(Throwable t)
      {
        clown.getEventLog().alarm(String.format("BlockDownloadThread-%s Error - ", name) + t);
        t.printStackTrace();
      }
      try
      {
        Thread.sleep(5000);
      }
      catch(Throwable t)
      {
        t.printStackTrace();
      }

    }
  }

  private void runInternal() throws Exception
  {
    if (broadcast_height == 0) broadcast_height = bitcoin_rpc.getBlockHeight() - clown.getConfig().getInt("blocks_back");

    int bitcoind_height = bitcoin_rpc.getBlockHeight();

    if (bitcoind_height != broadcast_height)
    {
      clown.getEventLog().log(String.format("Bitcoind-%s: %d Local %d", name, bitcoind_height, broadcast_height));
    }


    for(int i=broadcast_height+1; i<=bitcoind_height; i++)
    {
      downloadBlock(i);

    }

  }

  private void downloadBlock(int height)
    throws Exception
  {
    Sha256Hash hash = null;
    try
    {
      hash = bitcoin_rpc.getBlockHash(height);

      if (downloaded.contains(hash)) return;

      SerializedBlock block = bitcoin_rpc.getBlock(hash);
      Block b = block.getBlock(clown.getNetworkParameters());
      downloaded.add(hash);

      broadcast(height, hash, b);
      broadcast_height = height;
    }
    catch(Exception e)
    {
      clown.getEventLog().alarm(String.format("%s - error in download of block %d (%s) - %s", name, height, hash, e.toString()));
      throw e;
    }
  }

  private void broadcast(int height, Sha256Hash hash, Block b)
  {
    clown.getEventLog().log(String.format("Blocker-%s: Sharing %d %s", name, height, hash.toString()));

    Map<Sha256Hash, ByteString> tx_map = new HashMap<>();

    for(Transaction tx : b.getTransactions())
    {
      tx_map.put(tx.getHash(), ByteString.copyFrom(tx.bitcoinSerialize()));
    }

    my_broadcaster.addKnown(tx_map.keySet());
    clown.broadcastAll(tx_map);


  }
  

}
