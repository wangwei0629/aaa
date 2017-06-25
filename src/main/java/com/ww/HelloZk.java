package com.ww;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class HelloZk {

	private static final Logger logger = Logger.getLogger(HelloZk.class);

	private static final String CONNECTSTRING = "192.168.15.128:2181";
	private static final int SESSION_TIMEOUT = 60 * 1000;
	private static final String PATH = "/w77w";

	public ZooKeeper startZK() throws IOException {

		return new ZooKeeper(CONNECTSTRING, SESSION_TIMEOUT, new Watcher() {
			
			@Override
			public void process(WatchedEvent arg0) {
				
			}
		});
	}

	public void stopZK(ZooKeeper zk) throws InterruptedException {
		if (null != zk) {
			zk.close();
		}

	}

	public void createZNode(ZooKeeper zk, String nodePath, String nodeValue)
			throws KeeperException, InterruptedException {
		zk.create(nodePath, nodeValue.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

	}

	public String getZNode(ZooKeeper zk,String nodePath) throws KeeperException, InterruptedException {
		String retValue=null;
		byte[] byteArray = zk.getData(nodePath, false, new Stat());
		retValue=new String(byteArray); 
		return retValue;
	}

	public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
		HelloZk hZk=new HelloZk();
		ZooKeeper zk=hZk.startZK();
		
		if (zk.exists(PATH, false)==null ) {
			hZk.createZNode(zk, PATH, "hello");
			String retValue=hZk.getZNode(zk, PATH);
			logger.info("*************reValue"+retValue);
		} else {
			logger.info("*************I have this nodePath");
			
		}
		hZk.stopZK(zk);
	}
}
