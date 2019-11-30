package com.caselchen.flink;

import org.apache.curator.framework.CuratorFramework;

public class ZkClient {

    public static void main(String[] args) throws Exception {

        CuratorFramework zkClient = ZkServer.getZkClient();

        //第一次更新
        zkClient.setData().forPath(ZkServer.path, "first update".getBytes());
        Thread.sleep(1000);
        //第二次更新
        zkClient.setData().forPath(ZkServer.path, "second update".getBytes());
        Thread.sleep(1000);
        //第三次更新
        zkClient.setData().forPath(ZkServer.path, "third update".getBytes());
    }

}
