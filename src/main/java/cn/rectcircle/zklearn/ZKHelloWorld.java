package cn.rectcircle.zklearn;

import java.io.IOException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;

public class ZKHelloWorld {

	private static final int SESSION_TIMEOUT = 30000;
	private static final String addr = "192.168.3.21:2181";

	public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
		// watch是一次性触发
		Watcher watcher = (WatchedEvent event) -> {
			System.out.println("接收到事件："+event);
		};

		final ZooKeeper zk = new ZooKeeper(addr, SESSION_TIMEOUT, watcher);
		zk.exists("/zoo2/children1", true);

		System.out.println("创建ZooKeeper节点(path: /zoo2, data:myData2, acl:OPEN_ACL_UNSAFE, type: PERSISTENT)");
		zk.create("/zoo2", "myData2".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT); //监听触发

		System.out.println("查看是否创建成功");
		System.out.println(new String(zk.getData("/zoo2", false, null))); //没有重新设置监听

		System.out.println("修改数据节点");
		zk.setData("/zoo2", "update_Data".getBytes(), -1); //事件不会触发

		System.out.println("查看是否修改成功");
		System.out.println(new String(zk.getData("/zoo2", true, null))); //此时对/zoo2重新测试了监听

		zk.create("/zoo2/children1", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		System.out.println("查看/zoo2孩子列表");
		System.out.println(zk.getChildren("/zoo2", false));
		zk.delete("/zoo2/children1", -1);

		System.out.println("删除节点");
		zk.delete("/zoo2", -1);

		System.out.println("查看是否删除成功");
		System.out.println("节点状态: "+zk.exists("/zoo2", false));

		zk.close();
	}
}