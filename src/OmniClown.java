import com.google.protobuf.ByteString;
import org.bitcoinj.core.Sha256Hash;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import org.bitcoinj.core.NetworkParameters;
import org.bitcoinj.core.Context;
import org.bitcoinj.params.MainNetParams;


public class OmniClown
{
  

  public static void main(String args[]) throws Exception
  {
    new OmniClown(new Config(args[0])).start();
  }

  private EventLog event_log;
  private Config config;
  private ThreadPoolExecutor exec;
  private List<MemPooler> mempool_list;
  private List<Broadcaster> broad_list;
  private NetworkParameters network_params;
  private List<BlockDownloadThread> block_thread_list;


  public OmniClown(Config conf) throws Exception
  {
    this.config = conf;
    this.event_log = new EventLog(config);

    config.require("nodes");

    mempool_list = new LinkedList<>();
    broad_list = new LinkedList<>();
    block_thread_list = new LinkedList<>();
    network_params = MainNetParams.get();
    new Context(network_params);


    for(String name : config.getList("nodes"))
    {
      BitcoinRPC rpc = new BitcoinRPC(name, config, event_log);
      Broadcaster broad = new Broadcaster(this, rpc);
      MemPooler mempool = new MemPooler(this, rpc, broad);

      block_thread_list.add(new BlockDownloadThread(this, rpc, broad));

      broad_list.add(broad);
      mempool_list.add(mempool);
    
    }
  }

  private void start() throws Exception
  {
    exec =new ThreadPoolExecutor(16,16, 2, TimeUnit.DAYS, new SynchronousQueue<Runnable>(), new ThreadPoolExecutor.CallerRunsPolicy());

    for(MemPooler pool : mempool_list)
    {
      pool.start();
    }
    for(BlockDownloadThread t : block_thread_list)
    {
      t.start();
    }

  }

  public void broadcastAll(final Map<Sha256Hash, ByteString> tx_map)
  {
    for(final Broadcaster b : broad_list)
    {
      exec.execute(new Runnable(){
        public void run()
        {
          b.broadcast(tx_map);
        }
      });
    }
  }
  public int countLocal(Sha256Hash tx)
  {
    int n =0;
    for(Broadcaster b : broad_list)
    {
      if (b.hasLocal(tx)) n++;
    }
    return n;
  }


  public EventLog getEventLog() { return event_log; }
    public NetworkParameters getNetworkParameters()
        {
                return network_params;
                    }
  public Config getConfig() {return config;}
}
