package org.example.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Random;
import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

public class MasterAsyncImpl implements Watcher {

    ZooKeeper zk;
    String hostPort;
    static Random random = new Random();
    String serverId = Integer.toHexString(random.nextInt());
    boolean isLeader;

    public MasterAsyncImpl(String hostPort) {
        this.hostPort = hostPort;
    }

    void startZK() throws IOException {
        zk = new ZooKeeper(hostPort,15000,this);
    }

    void stopZK() throws InterruptedException {
        zk.close();
    }

    void checkMaster() throws InterruptedException, KeeperException {
        zk.getData("/master", false, masterCheckCallback, null);
    }

    void runForMaster() throws InterruptedException, KeeperException {
        zk.create("/master", serverId.getBytes(), OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, masterCreateCallback, null);
    }

    AsyncCallback.StringCallback masterCreateCallback = new AsyncCallback.StringCallback() {
        @Override
        public void processResult(int i, String s, Object o, String s1) {
            switch (KeeperException.Code.get(i)){
                case CONNECTIONLOSS:
                    try {
                        checkMaster();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    } catch (KeeperException e) {
                        throw new RuntimeException(e);
                    }
                    return;
                case OK:
                    isLeader = true;
                    break;

                default:
                    isLeader = false;
            }
            System.out.println("I am " + (isLeader ? "" : "not ") + "the leader in Async Impl");
        }
    };

    AsyncCallback.DataCallback masterCheckCallback = new AsyncCallback.DataCallback() {
        @Override
        public void processResult(int i, String s, Object o, byte[] bytes, Stat stat) {
            switch (KeeperException.Code.get(i)){
                case CONNECTIONLOSS :
                    try {
                        checkMaster();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    } catch (KeeperException e) {
                        throw new RuntimeException(e);
                    }
                    return;
                case NONODE:
                    try {
                        runForMaster();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    } catch (KeeperException e) {
                        throw new RuntimeException(e);
                    }
                    return;
            }
        }
    };



    @Override
    public void process(WatchedEvent watchedEvent) {
        System.out.println(watchedEvent);
    }

    public static void main(String[] args) throws Exception {
        MasterAsyncImpl m = new MasterAsyncImpl(args[0]);
        m.startZK();

        m.runForMaster();

        if(m.isLeader) {
            System.out.println("I am the leader in Async Impl");
            Thread.sleep(60000);
        } else {
           System.out.println("Someone else is the leader in Async Impl");
        }
        m.stopZK();
    }
}

