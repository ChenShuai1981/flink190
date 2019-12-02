//package com.caselchen.flink;
//
//import org.apache.curator.framework.recipes.cache.NodeCache;
//import org.apache.curator.framework.recipes.cache.NodeCacheListener;
//import org.apache.curator.framework.CuratorFramework;
//import org.apache.curator.framework.CuratorFrameworkFactory;
//import org.apache.curator.retry.ExponentialBackoffRetry;
//
//public class Watcher {
//
//    public static void main(String[] args) throws Exception {
//
//        CuratorFramework zkClient = getZkClient();
//
//        String path = "/nodeCache";
//        byte[] initData = "initData".getBytes();
//
//        //创建节点用于测试
//        zkClient.create().forPath(path, initData);
//
//        NodeCache nodeCache = new NodeCache(zkClient, path);
//        //调用start方法开始监听
//        nodeCache.start();
//
//        //添加NodeCacheListener监听器
//        nodeCache.getListenable().addListener(new NodeCacheListener() {
//            @Override
//            public void nodeChanged() throws Exception {
//                System.out.println("监听到事件变化，当前数据:"+new String(nodeCache.getCurrentData().getData()));
//            }
//        });
//
//        //第一次更新
//        zkClient.setData().forPath(path, "first update".getBytes());
//        Thread.sleep(1000);
//        //第二次更新
//        zkClient.setData().forPath(path, "second update".getBytes());
//        Thread.sleep(1000);
//        //第三次更新
//        zkClient.setData().forPath(path, "third update".getBytes());
//
//        Thread.sleep(Integer.MAX_VALUE);
//    }
//
//    private static CuratorFramework getZkClient() {
//        String zkServerAddress = "127.0.0.1:2181";
//        ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(1000, 3, 5000);
//        CuratorFramework zkClient = CuratorFrameworkFactory.builder()
//                .connectString(zkServerAddress)
//                .sessionTimeoutMs(5000)
//                .connectionTimeoutMs(5000)
//                .retryPolicy(retryPolicy)
//                .build();
//        zkClient.start();
//        return zkClient;
//    }
//
//}
