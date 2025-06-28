package org.example.zookeeper.coordination;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Random;

import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;


public class Worker implements Watcher {

    private static final Logger LOG = LoggerFactory.getLogger(Worker.class);
    ZooKeeper zk;
    String hostPort;
    String status;
    static Random random = new Random();
    static String serverId = Integer.toHexString(random.nextInt());
    String name = "worker-" + serverId;

    public Worker(String hostPort) {
        this.hostPort = hostPort;
    }

    void startZK() throws IOException {
        zk = new ZooKeeper(hostPort, 15000, this);
    }

    void register(){
        zk.create("/workers/" + name, "Idle".getBytes(), OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, createWorkerCallback, null);
    }

    public void setStatus(String status) {
        this.status = status;
        updateStatus(status);
    }

    synchronized private void updateStatus(String status){
        if(status == this.status) {
            zk.setData("/workers/" + name, status.getBytes(), -1, statusUpdateCallback, status);
        }
    }

    AsyncCallback.StatCallback statusUpdateCallback = new AsyncCallback.StatCallback() {
        @Override
        public void processResult(int i, String s, Object o, Stat stat) {
            switch (KeeperException.Code.get(i)) {
                case CONNECTIONLOSS:
                    updateStatus((String) o);
                    return;
            }
        }
    };

    AsyncCallback.StringCallback createWorkerCallback = new AsyncCallback.StringCallback() {
        @Override
        public void processResult(int i, String s, Object o, String s1) {
            switch (KeeperException.Code.get(i)){
                case CONNECTIONLOSS :
                    register();
                    break;
                case OK:
                    LOG.info("Registered successfully: " + serverId);
                    break;
                case NODEEXISTS:
                    LOG.warn("Already registered: " + serverId);
                    break;
                default:
                    LOG.error("Something went wrong: " + KeeperException.create(KeeperException.Code.get(i),s));
            }
        }
    };

    public static void main(String[] args) throws Exception {
        Worker worker = new Worker(args[0]);
        worker.startZK();
        worker.register();
        Thread.sleep(30000);
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        LOG.info("Watched Event is now : " + watchedEvent.toString());
        System.out.println(watchedEvent);
    }
}
