package com.caselchen.flink;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.ToString;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

public class TimingWheelTest {
    public static void main(String[] args) throws Exception {
        // Create a timing-wheel with 60 ticks, and every tick is 1 second.
        TimingWheel<CallEvent> TIMING_WHEEL = new TimingWheel<CallEvent>(1, 30, TimeUnit.SECONDS);

        // Add expiration listener and start the timing-wheel.
        TIMING_WHEEL.addExpirationListener(new ExpirationListener<CallEvent>() {
            @Override
            public void expired(CallEvent expiredObject) {
                System.out.println("[Expire] " + System.currentTimeMillis() + " -> " + expiredObject.getPhoneNo() + " <- " + expiredObject.getTimestamp());
                System.out.println("after expire >> " + TIMING_WHEEL.size());
            }
        });
        TIMING_WHEEL.start();

        // Add one element to be timeout approximated after 60 seconds
        for (int i=0; i<5; i++) {
            TIMING_WHEEL.add(new CallEvent("13801899719", System.currentTimeMillis()));
            System.out.println(TIMING_WHEEL.size());
            Thread.sleep(2000);
        }

        // Anytime you can cancel count down timer for element e like this
//     TIMING_WHEEL.remove(e);
    }

    @Data
    @AllArgsConstructor
    @ToString
    static class CallEvent implements Serializable {
        private String phoneNo;
        private long timestamp;
    }
}
