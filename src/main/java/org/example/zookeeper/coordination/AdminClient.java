package org.example.zookeeper.coordination;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Date;

public class AdminClient implements Watcher {

    ZooKeeper zk;
    String hostPort;

    public AdminClient(String hostPort) {
        this.hostPort = hostPort;
    }

    void start() throws Exception {
        zk = new ZooKeeper(hostPort, 15000, this);
    }

    void listState() throws KeeperException, InterruptedException {
        try {
            Stat stat = new Stat();
            byte[] masterData = zk.getData("/master", false, stat);
            Date startDate = new Date(stat.getCtime());
            System.out.println("Master: " + new String(masterData) + " since " + startDate);
        } catch (KeeperException.NoNodeException e){
            System.out.println("No Master");
        }
        System.out.println("Workers: ");
        for(String w: zk.getChildren("/workers", false)) {
            byte[] data = zk.getData("/workers/" + w, false, null);
            String state = new String(data);
            System.out.println("\t" + w + ": " + state);
        }

        System.out.println("Tasks: ");
        for(String t: zk.getChildren("/tasks", false)){
            System.out.println("\t" + t);
        }
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        System.out.println(watchedEvent);
    }

    public static void main(String[] args) throws Exception{
        AdminClient c = new AdminClient(args[0]);

        c.start();

        c.listState();
    }
}
