package com.hl.bigdata.flume;

import org.apache.flume.Channel;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.sink.AbstractSink;

/* * 
 * 自定义sink
 * @Author: huanglin 
 * @Date: 2022-02-28 21:53:22 
 */ 
public class SelfSink extends AbstractSink {

    @Override
    public Status process() throws EventDeliveryException {
        Channel     channel     = getChannel();
        Transaction transaction = channel.getTransaction();
        try {
            transaction.begin();
            Event event = channel.take();
            if(event != null) {
                String str = new String(event.getBody());
                System.out.println(str);
            }
            transaction.commit();
            return Status.READY;
        } catch (Exception e) {
            transaction.rollback();
        } finally {
            transaction.close();
        }

        return Status.BACKOFF;
    }
}
