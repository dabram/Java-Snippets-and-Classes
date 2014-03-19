import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A specific Queue Class that behaves similarly to PriorityBlockingQueue.
 * This Queue blocks the put() if the Cap of the Queue is reached.
 * This Queue blocks the take() if the Queue is empty.
 * Useful for multithreaded Producer-Consumer Problem
 * For the put() and take() Blocking it uses the Implementation of LinkedBlockingQueue()
 * 
 * For List of Feautures see http://danielabram.de or readme at Github https://github.com/dabram/Java-Snippets-and-Classes
 *
 * @author Daniel Abram
 */

public class PriorityBlockingCapacityQueue<E> extends PriorityBlockingQueue<E>{

	private static final long serialVersionUID = 1L;
	private final int capacity;
	private final ReentrantLock putLock = new ReentrantLock();
	private final Condition notFull = putLock.newCondition();
	private final AtomicInteger count = new AtomicInteger(0);
	private final ReentrantLock takeLock = new ReentrantLock();
	private final Condition notEmpty = takeLock.newCondition();
	
	
	/**
	 * Constructs a fully qualified PriorityBlockingCapacityQueue
	 * 
	 * @param cap the Maximum Size of the Queue before put() gets Blocked
	 */
	public PriorityBlockingCapacityQueue(int cap){
		super();
		this.capacity = cap;
	}
	
	/**
	 * Writes an Object into the Queue, checks if the Queue is full or not, blocks if it is full and waits 
	 * 
	 * @param e the Object that is going to be written into the Queue
	 */
	public void put(E e){
		if (e == null) throw new NullPointerException();
        // Note: convention in all put/take/etc is to preset
        // local var holding count  negative to indicate failure unless set.
        int c = -1;
        final ReentrantLock putLock = this.putLock;
        final AtomicInteger count = this.count;
        try {
			putLock.lockInterruptibly();
		} catch (InterruptedException e1) {
			//Here goes the Code for Logging or similar Behaviour
			//e.g.: "Could not lock Interruptibly in PriorityBlockingCapacityQueue on ReentrantLock"
		}
        try {
            /*
             * Note that count is used in wait guard even though it is
             * not protected by lock. This works because count can
             * only decrease at this point (all other puts are shut
             * out by lock), and we (or some other waiting put) are
             * signalled if it ever changes from
             * capacity. Similarly for all other uses of count in
             * other wait guards.
             */
            try {
                while (count.get() == capacity)
                    notFull.await();
            } catch (InterruptedException ie) {
                	notFull.signal(); // propagate to a non-interrupted thread
			//Here goes the Code for Logging or similar Behaviour
                	//e.g.: "Could not wait! in Lockmechanism"
            }
            super.offer(e);
            c = count.getAndIncrement();
            if (c + 1 < capacity)
                notFull.signal();
        } finally {
            putLock.unlock();
        }
        if (c == 0)
            signalNotEmpty();
	}
	
	private void signalNotEmpty() {
        final ReentrantLock takeLock = this.takeLock;
        takeLock.lock();
        try {
            notEmpty.signal();
        } finally {
            takeLock.unlock();
        }
    }
	
	/**
	 * Blocks if Queue is empty
	 * 
	 * @return E the Object from the Queue at the Head
	 */
	public E take(){
		E x = null;
        int c = -1;
        final AtomicInteger count = this.count;
        final ReentrantLock takeLock = this.takeLock;
        try {
			takeLock.lockInterruptibly();
		} catch (InterruptedException e) {
			//Here goes the Code for Logging or similar Behaviour
			//e.g.: "Could not lock Interruptibly in PriorityBlockingCapacityQueue on ReentrantLock"
		}
        try {
            try {
                while (count.get() == 0)
                    notEmpty.await();
            } catch (InterruptedException ie) {
                	notEmpty.signal(); // propagate to a non-interrupted thread
			//Here goes the Code for Logging or similar Behaviour
			//e.g.: "Could await Condition"
				
            }

            try {
				x = super.take();
			} catch (InterruptedException e) {
				//Here goes the Code for Logging or similar Behaviour
				//e.g.: "Could not take Element from Queue"
			}
            c = count.getAndDecrement();
            if (c > 1)
                notEmpty.signal();
        } finally {
            takeLock.unlock();
        }
        if (c == capacity)
            signalNotFull();
        return x;
	}
	
	private void signalNotFull() {
        final ReentrantLock putLock = this.putLock;
        putLock.lock();
        try {
            notFull.signal();
        } finally {
            putLock.unlock();
        }
    }
}
