package org.example.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Random;

import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

public class MasterSyncImpl implements Watcher {

    ZooKeeper zk;
    String hostPort;
    static Random random = new Random();
    String serverId = Integer.toHexString(random.nextInt());
    boolean isLeader;

    public MasterSyncImpl(String hostPort) {
        this.hostPort = hostPort;
    }

    void startZK() throws IOException {
        zk = new ZooKeeper(hostPort,15000,this);
    }

    void stopZK() throws InterruptedException {
        zk.close();
    }

    boolean checkMaster() throws InterruptedException, KeeperException {
        while(true) {
            try{
                Stat stat = new Stat();
                byte[] data = zk.getData("/master", false, stat);
                isLeader = new String(data).equals(serverId);
            } catch (KeeperException.NoNodeException e) {
                //no master, so try create again
                return false;
            } catch (KeeperException.ConnectionLossException e){

            }
        }
    }

    void runForMaster() throws InterruptedException, KeeperException {
        while (true){
            try {
                zk.create("/master", serverId.getBytes(), OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                isLeader = true;
                break;
            } catch (KeeperException.NodeExistsException e){
                isLeader = false;
                break;
            } catch (KeeperException.ConnectionLossException e){

            }
            if(checkMaster())
                break;
        }

    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        System.out.println(watchedEvent);
    }

    public static void main(String[] args) throws Exception {
        MasterSyncImpl m = new MasterSyncImpl(args[0]);
        m.startZK();

        m.runForMaster();

        if(m.isLeader) {
            System.out.println("I am the leader in Sync Impl");
            Thread.sleep(60000);
        } else {
            System.out.println("Someone else is the leader in Sync Impl");
        }
        m.stopZK();
    }
}

