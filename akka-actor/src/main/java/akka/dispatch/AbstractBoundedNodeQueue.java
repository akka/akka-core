/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.dispatch;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.lang.reflect.Field;

/**
 * Lock-free bounded non-blocking multiple-producer single-consumer queue based on the works of:
 *
 * Andriy Plokhotnuyk (https://github.com/plokhotnyuk)
 *   - https://github.com/plokhotnyuk/actors/blob/2e65abb7ce4cbfcb1b29c98ee99303d6ced6b01f/src/test/scala/akka/dispatch/Mailboxes.scala
 *     (Apache V2: https://github.com/plokhotnyuk/actors/blob/master/LICENSE)
 *
 * Dmitriy Vyukov's non-intrusive MPSC queue:
 *   - https://www.1024cores.net/home/lock-free-algorithms/queues/non-intrusive-mpsc-node-based-queue
 *   (Simplified BSD)
 */
@SuppressWarnings("serial")
public abstract class AbstractBoundedNodeQueue<T> {
    private final int capacity;

    @SuppressWarnings("unused")
    private volatile Node<T> _enqDoNotCallMeDirectly;

    @SuppressWarnings("unused")
    private volatile Node<T> _deqDoNotCallMeDirectly;

    protected AbstractBoundedNodeQueue(final int capacity) {
        if (capacity < 0) throw new IllegalArgumentException("AbstractBoundedNodeQueue.capacity must be >= 0");
        this.capacity = capacity;
        final Node<T> n = new Node<T>();
        setDeq(n);
        setEnq(n);
    }

    private void setEnq(Node<T> n) {
      enqHandle.setVolatile(this, n);
    }

    @SuppressWarnings("unchecked")
    private Node<T> getEnq() {
        return (Node<T>) enqHandle.getVolatile(this);
    }

    private boolean casEnq(Node<T> old, Node<T> nju) {
        return enqHandle.compareAndSet(this, old, nju);
    }

    private void setDeq(Node<T> n) {
      deqHandle.setVolatile(this, n);
    }

    @SuppressWarnings("unchecked")
    private Node<T> getDeq() {
        return (Node<T>)deqHandle.getVolatile(this);
    }

    private boolean casDeq(Node<T> old, Node<T> nju) {
        return deqHandle.compareAndSet(this, old, nju);
    }

    protected final Node<T> peekNode() {
        for(;;) {
          final Node<T> deq = getDeq();
          final Node<T> next = deq.next();
          if (next != null || getEnq() == deq)
            return next;
        }
    }

    /**
     *
     * @return the first value of this queue, null if empty
     */
    public final T peek() {
        final Node<T> n = peekNode();
        return (n != null) ? n.value : null;
    }

    /**
     * @return the maximum capacity of this queue
     */
    public final int capacity() {
        return capacity;
    }
    // Possible TODO — impl. could be switched to addNode(new Node(value)) if we want to allocate even if full already
    public final boolean add(final T value) {
        for(Node<T> n = null;;) {
            final Node<T> lastNode = getEnq();
            final int lastNodeCount = lastNode.count;
            if (lastNodeCount - getDeq().count < capacity) {
              // Trade a branch for avoiding to create a new node if full,
              // and to avoid creating multiple nodes on write conflict á la Be Kind to Your GC
              if (n == null) {
                  n = new Node<T>();
                  n.value = value;
              }

              n.count = lastNodeCount + 1; // Piggyback on the HB-edge between getEnq() and casEnq()

              // Try to append the node to the end, if we fail we continue loopin'
              if(casEnq(lastNode, n)) {
                  lastNode.setNext(n);
                  return true;
              }
            } else return false; // Over capacity—couldn't add the node
        }
    }

     public final boolean addNode(final Node<T> n) {
         n.setNext(null); // Make sure we're not corrupting the queue
         for(;;) {
             final Node<T> lastNode = getEnq();
             final int lastNodeCount = lastNode.count;
             if (lastNodeCount - getDeq().count < capacity) {
                 n.count = lastNodeCount + 1; // Piggyback on the HB-edge between getEnq() and casEnq()
                 // Try to append the node to the end, if we fail we continue loopin'
                 if(casEnq(lastNode, n)) {
                     lastNode.setNext(n);
                     return true;
                 }
             } else return false; // Over capacity—couldn't add the node
         }
    }

    public final boolean isEmpty() {
        return getEnq() == getDeq();
    }

    /**
     * Returns an approximation of the queue's "current" size
     */
    public final int size() {
        //Order of operations is extremely important here
        // If no item was dequeued between when we looked at the count of the enqueuing end,
        // there should be no out-of-bounds
        for(;;) {
            final int deqCountBefore = getDeq().count;
            final int enqCount = getEnq().count;
            final int deqCountAfter = getDeq().count;

            if (deqCountAfter == deqCountBefore)
                return enqCount - deqCountAfter;
        }
    }

    /**
     * Removes the first element of this queue if any
     * @return the value of the first element of the queue, null if empty
     */
    public final T poll() {
        final Node<T> n = pollNode();
        return (n != null) ? n.value : null;
    }

    /**
     * Removes the first element of this queue if any
     * @return the `Node` of the first element of the queue, null if empty
     */
    public final Node<T> pollNode() {
        for(;;) {
            final Node<T> deq = getDeq();
            final Node<T> next = deq.next();
            if (next != null) {
                if (casDeq(deq, next)) {
                    deq.value = next.value;
                    deq.setNext(null);
                    next.value = null;
                    return deq;
                } // else we retry (concurrent consumers)
            } else if (getEnq() == deq) return null; // If we got a null and head meets tail, we are empty
        }
    }

    private final static VarHandle enqHandle;
    private final static VarHandle deqHandle;

    static {
        try {
          MethodHandles.Lookup lookup = MethodHandles.privateLookupIn(AbstractBoundedNodeQueue.class, MethodHandles.lookup());
          Field enqField = AbstractBoundedNodeQueue.class.getDeclaredField("_enqDoNotCallMeDirectly");
          enqHandle = lookup.unreflectVarHandle(enqField);

          Field deqField = AbstractBoundedNodeQueue.class.getDeclaredField("_deqDoNotCallMeDirectly");
          deqHandle = lookup.unreflectVarHandle(deqField);
        } catch(Throwable t){
            throw new ExceptionInInitializerError(t);
        }
    }

    public static class Node<T> {
        protected T value;
        @SuppressWarnings("unused")
        private volatile Node<T> _nextDoNotCallMeDirectly;
        protected int count;

        @SuppressWarnings("unchecked")
        public final Node<T> next() {
            return (Node<T>) nextHandle.getVolatile(this);
        }

        protected final void setNext(final Node<T> newNext) {
          nextHandle.setRelease(this, newNext);
        }
        
        private final static VarHandle nextHandle;
        
        static {
            try {
              MethodHandles.Lookup lookup = MethodHandles.privateLookupIn(Node.class, MethodHandles.lookup());
              Field nextField = Node.class.getDeclaredField("_nextDoNotCallMeDirectly");
              nextHandle = lookup.unreflectVarHandle(nextField);
            } catch(Throwable t){
                throw new ExceptionInInitializerError(t);
            } 
        }
    } 
}
