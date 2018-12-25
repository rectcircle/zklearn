package cn.rectcircle.zklearn;


public class ZKLockException extends RuntimeException{

	private static final long serialVersionUID = 1L;

	public ZKLockException(Throwable e) {
		super(e);
	}

	public ZKLockException(String msg) {
		super(msg);
	}

}
