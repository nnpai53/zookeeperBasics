package org.example.zookeeper.coordination;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Random;

import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;


public class Master implements Watcher{

    ZooKeeper zk;
    String hostPort;

    static Random random = new Random();
    String serverId = Integer.toHexString(random.nextInt());
    boolean isLeader;


    private static final Logger LOG = LoggerFactory.getLogger(Master.class);


    public Master(String hostPort) {
        this.hostPort = hostPort;
    }

    void startZK() throws IOException {
        zk = new ZooKeeper(hostPort,15000,this);
    }

    void stopZK() throws InterruptedException {
        zk.close();
    }

    public void bootstrap(){
        createParent("/workers", new byte[0]);
        createParent("/assign", new byte[0]);
        createParent("/tasks", new byte[0]);
        createParent("/status", new byte[0]);
    }

    void createParent(String path, byte[] data){
        zk.create(path,data, OPEN_ACL_UNSAFE, CreateMode.PERSISTENT,createParentCallback, data);
    }

    AsyncCallback.StringCallback createParentCallback = new AsyncCallback.StringCallback() {
        @Override
        public void processResult(int i, String s, Object o, String s1) {
            switch (KeeperException.Code.get(i)){
                case CONNECTIONLOSS:
                    createParent(s,(byte[]) o);
                    break;
                case OK:
                    LOG.info("Parent created");
                    break;
                case NODEEXISTS:
                    LOG.warn("Parent already registered: " + s);
                    break;
                default:
                    LOG.error("Something went wrong: ", KeeperException.create(KeeperException.Code.get(i),s));
            }
        }
    };


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
            System.out.println("I am " + (isLeader ? "" : "not ") + "the leader");
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
        LOG.info("Watched Event is now : " + watchedEvent.toString());
        System.out.println(watchedEvent);
    }

    public static void main(String[] args) throws Exception {
        Master m = new Master(args[0]);
        m.startZK();
        m.runForMaster();

        m.bootstrap();
        m.stopZK();
    }
}
