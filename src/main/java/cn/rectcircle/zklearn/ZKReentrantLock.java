package cn.rectcircle.zklearn;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

/**
 * zk实现的进程级别的锁，不可以直接在多线程中使用，且不支持严格可重入（同一线程可以多次获取，但是一次释放全部）
 */
class ZKProcessLock implements Lock {
	// 锁根节点
	private static final String ROOT_LOCK = "/locks";
	private static final String SPLIT_STRING = "_lock_";

	// 会话超时事件
	private static final int SESSION_TIMEOUT = 30000;
	// zk客户端
	private final ZooKeeper zk;
	// 当前锁名
	private final String lockName;
	// 监听的事件
	private final Watcher watcher = (WatchedEvent event) -> {
		System.out.println("接收到事件：" + event);
		if (this.countDownLatch != null) {
			this.countDownLatch.countDown();
		}
	};
	// 用于阻塞等待watch的时间
	private CountDownLatch countDownLatch;
	// 等待锁的key
	private String waitLockKey;
	// 当前进程锁的key
	private String currentLockKey;

	/**
	 * 配置分布式锁
	 * 
	 * @param addr     连接的url
	 * @param lockName 锁名
	 * @throws IOException
	 */
	public ZKProcessLock(String addr, String lockName) {
		try {
			this.zk = new ZooKeeper(addr, SESSION_TIMEOUT, watcher);
			this.lockName = lockName;
			// 这将触发一个watched事件
			Stat stat = zk.exists(ROOT_LOCK, false);
			// 检测是否创建了锁目录，若是没有创建，则创建。
			if (stat == null) {
				zk.create(ROOT_LOCK, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			}
		} catch (IOException | KeeperException | InterruptedException e) {
			throw new ZKLockException(e);
		}
	}

	private boolean waitForLock(long waitTime, TimeUnit unit) throws InterruptedException {
		if (this.countDownLatch == null) {
			this.countDownLatch = new CountDownLatch(1);
		}
		boolean result = false;
		try {
			Stat stat = this.zk.exists(this.waitLockKey, true);
			if (stat != null) {
				System.out.println(Thread.currentThread().getName() + "等待进程锁 " + this.waitLockKey);
				if (waitTime != 0) {
					result = this.countDownLatch.await(waitTime, unit);
				} else {
					this.countDownLatch.await();
					result = true;
				}
			}
		} catch (KeeperException e) {
			throw new ZKLockException(e);
		} finally {
			if (result) {
				this.countDownLatch = null;
				System.out.println(Thread.currentThread().getName() + " 获得到了进程锁");
			}
		}
		return result;
	}

	@Override
	public void lock() {
		try {
			lockInterruptibly();
		} catch (InterruptedException e) {
			lock();
		}
	}

	@Override
	public void lockInterruptibly() throws InterruptedException {
		if (this.writeAndCheckZK()) {
			System.out.println(Thread.currentThread().getName() + " " + lockName + "获得了进程锁");
			return;
		}
		try {
			if (waitForLock(0, null)) {
				return;
			}
			unlock();
		} catch (Exception e) {
			unlock();
			throw e;
		} 
	}

	private boolean writeAndCheckZK(){
		if (this.lockName.contains(SPLIT_STRING)) {
			throw new ZKLockException("锁名有误");
		}
		// 创建临时有序节点
		try {
			if (this.currentLockKey == null) {
				this.currentLockKey = zk.create(ROOT_LOCK + "/" + this.lockName + SPLIT_STRING, new byte[0],
						ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
			}
			System.out.println(this.currentLockKey + " 已经创建");
			// 取所有子节点
			List<String> lockKeys = zk.getChildren(ROOT_LOCK, false);
			String currentKey = this.currentLockKey.replace(ROOT_LOCK + "/", "");
			// 如果存在小于currentKey的值，则返回其中的最大值
			// 否则返回currentKey
			String waitOrCurrentKey = lockKeys.stream()
					.filter(s -> s.startsWith(this.lockName) && s.compareTo(currentKey) < 0)
					.reduce((s1, s2) -> s1.compareTo(s2) >= 0 ? s1 : s2).orElse(currentKey);
			// 若当前节点为最小节点，则获取锁成功
			if (this.currentLockKey.equals(ROOT_LOCK + "/" + waitOrCurrentKey)) {
				return true;
			}
			// 若不是最小节点，则找到自己的前一个节点
			this.waitLockKey = ROOT_LOCK + "/" + waitOrCurrentKey;
		} catch (KeeperException | InterruptedException e) {
			e.printStackTrace();
		}
		return false;
	}

	@Override
	public boolean tryLock() {
		if (writeAndCheckZK()){
			return true;
		}
		unlock();
		return false;
	}

	@Override
	public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
		if (this.writeAndCheckZK()) {
			return true;
		}
		try {
			if (waitForLock(time, unit)) {
				return true;
			}
			unlock();
			return false;
		} catch (InterruptedException e) {
			unlock();
			throw e;
		}
	}

	@Override
	public void unlock() {
		try {
			if(this.currentLockKey!=null){
				this.zk.delete(this.currentLockKey, -1);
				this.currentLockKey = null;
				this.waitLockKey = null;
				this.countDownLatch = null;
			}
		} catch (KeeperException e) {
			throw new ZKLockException(e);
		} catch (InterruptedException e){
			unlock();
		}
	}

	@Override
	public Condition newCondition() {
		throw new UnsupportedOperationException("ZK分布式锁暂不支持条件变量");
	}

}

/**
 * ZKProcessLock + ReentrantLock 实现的可重入分布式锁
 */
public class ZKReentrantLock implements Lock {

	private final ZKProcessLock processLock;
	private final ReentrantLock threadLock;
	private int  count;

	public ZKReentrantLock(String addr, String lockName) {
		this.processLock = new ZKProcessLock(addr, lockName);
		this.threadLock = new ReentrantLock();
		this.count = 0;
	}

	@Override
	public void lock() {
		threadLock.lock();
		if(this.count++ > 0){
			return;
		}
		processLock.lock();
	}

	@Override
	public void lockInterruptibly() throws InterruptedException {
		try {
			threadLock.lockInterruptibly();
			if (this.count++ > 0) {
				return;
			}
		} catch (InterruptedException e) {
			throw e;
		}
		try {
			processLock.lockInterruptibly();
		} catch (InterruptedException e) {
			this.unlock();
			throw e;
		}


	}

	@Override
	public boolean tryLock() {
		if (threadLock.tryLock()){
			if (this.count++ > 0) {
				return true;
			}
			if(processLock.tryLock()){
				return true;
			} else {
				this.unlock();
			}
		} 
		return false;
	}

	@Override
	public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
		if (threadLock.tryLock(time, unit)){
			if (this.count++ > 0) {
				return true;
			}
			if (processLock.tryLock(time, unit)) {
				return true;
			} else {
				unlock();
			}
		}
		return false;
	}

	@Override
	public void unlock() {
		if(--this.count==0){
			processLock.unlock();
		}
		threadLock.unlock();
	}

	@Override
	public Condition newCondition() {
		throw new UnsupportedOperationException("ZK分布式锁暂不支持条件变量");
	}

	public static void main(String[] args) throws IOException, InterruptedException {
		String addr = "192.168.3.21:2181";

		// Lock lock = new ZKProcessLock(addr, "test1");
		// lock.lock();
		// lock.lock();
		// System.out.println("---");
		// Thread.sleep(1000);

		// Lock lock1 = new ZKProcessLock(addr, "test1");
		// Lock lock2 = new ZKProcessLock(addr, "test1");
		// lock1.lock();
		// Thread t = new Thread(()->{
		// 	// lock2.lock();
		// 	try {
		// 		lock2.lockInterruptibly();
		// 	} catch (InterruptedException e) {
		// 		e.printStackTrace();
		// 	}
		// 	lock2.unlock();
		// });
		// t.start();
		// Thread.sleep(1000);
		// t.interrupt();
		// lock1.unlock();
		// System.out.println("===");
		// Thread.sleep(10000);

		Lock lock = new ZKReentrantLock(addr, "test2");
		lock.lock();
		lock.lock();
		lock.unlock();
		Thread t = new Thread(()->{
			lock.lock();
			System.out.println(Thread.currentThread().getName()+"加锁一次");
			lock.unlock();
		});
		t.start();
		System.out.println(Thread.currentThread()+"加锁2次，解锁一次");
		Thread.sleep(1000);
		lock.unlock();
		System.out.println("===");
	}

}