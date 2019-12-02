package com.caselchen.flink;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;

public class ZkServer {

    public static final String path = "/nodeCache";

    public static void main(String[] args) throws Exception {
        CuratorFramework zkClient = getZkClient();

        byte[] initData = "initData".getBytes();

        zkClient.delete().forPath(path);

        //创建节点用于测试
        zkClient.create().forPath(path, initData);

        NodeCache nodeCache = new NodeCache(zkClient, path);

        //添加NodeCacheListener监听器
        nodeCache.getListenable().addListener(new NodeCacheListener() {
            @Override
            public void nodeChanged() throws Exception {
                System.out.println("监听到事件变化，当前数据:"+new String(nodeCache.getCurrentData().getData()));
            }
        });

        //调用start方法开始监听
        nodeCache.start();

        // 模拟服务器一直运行
        Thread.sleep(Long.MAX_VALUE);
    }

    public static CuratorFramework getZkClient() {
        String zkServerAddress = "127.0.0.1:2181";
        ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(1000, 3, 5000);
        CuratorFramework zkClient = CuratorFrameworkFactory.builder()
                .connectString(zkServerAddress)
                .sessionTimeoutMs(5000)
                .connectionTimeoutMs(5000)
                .retryPolicy(retryPolicy)
                .build();
        zkClient.start();
        return zkClient;
    }
}
