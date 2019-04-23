/*
 * Copyright 1999-2015 dangdang.com.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * </p>
 */

package io.elasticjob.lite.reg.zookeeper;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.elasticjob.lite.reg.base.CoordinatorRegistryCenter;
import io.elasticjob.lite.reg.exception.RegExceptionHandler;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.ACLProvider;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

/**
 * 基于Zookeeper的注册中心.
 * 使用 Apache Curator 进行 Zookeeper 注册中心
 * 
 * @author zhangliang
 */
@Slf4j
public final class ZookeeperRegistryCenter implements CoordinatorRegistryCenter {
    
    @Getter(AccessLevel.PROTECTED)
    private ZookeeperConfiguration zkConfig;
    
    private final Map<String, TreeCache> caches = new HashMap<>();
    
    @Getter
    private CuratorFramework client;
    
    public ZookeeperRegistryCenter(final ZookeeperConfiguration zkConfig) {
        this.zkConfig = zkConfig;
    }
    
    @Override
    public void init() {
        log.debug("Elastic job: zookeeper registry center init, server lists is: {}.", zkConfig.getServerLists());
        CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder()
                .connectString(zkConfig.getServerLists())

                //ExponentialBackoffRetry，
                // 当 Zookeeper 失去链接后重新连接的一种策略：动态计算每次计算重连的间隔，
                // 时间间隔 = baseSleepTimeMs * Math.max(1, random.nextInt(1 << (retryCount + 1)))
                .retryPolicy(new ExponentialBackoffRetry(zkConfig.getBaseSleepTimeMilliseconds(), zkConfig.getMaxRetries(), zkConfig.getMaxSleepTimeMilliseconds()))
                .namespace(zkConfig.getNamespace());
        if (0 != zkConfig.getSessionTimeoutMilliseconds()) {
            builder.sessionTimeoutMs(zkConfig.getSessionTimeoutMilliseconds());
        }
        if (0 != zkConfig.getConnectionTimeoutMilliseconds()) {
            builder.connectionTimeoutMs(zkConfig.getConnectionTimeoutMilliseconds());
        }

        //认证，默认为不需要权限验证
        if (!Strings.isNullOrEmpty(zkConfig.getDigest())) {
            builder.authorization("digest", zkConfig.getDigest().getBytes(Charsets.UTF_8))
                    .aclProvider(new ACLProvider() {
                    
                        @Override
                        public List<ACL> getDefaultAcl() {
                            return ZooDefs.Ids.CREATOR_ALL_ACL;
                        }
                    
                        @Override
                        public List<ACL> getAclForPath(final String path) {
                            return ZooDefs.Ids.CREATOR_ALL_ACL;
                        }
                    });
        }
        client = builder.build();
        client.start();
        //连接zk
        try {
            if (!client.blockUntilConnected(zkConfig.getMaxSleepTimeMilliseconds() * zkConfig.getMaxRetries(), TimeUnit.MILLISECONDS)) {
                client.close();
                throw new KeeperException.OperationTimeoutException();
            }
            //CHECKSTYLE:OFF
        } catch (final Exception ex) {
            //CHECKSTYLE:ON
            RegExceptionHandler.handleException(ex);
        }
    }
    
    @Override
    public void close() {
        for (Entry<String, TreeCache> each : caches.entrySet()) {
            each.getValue().close();
        }
        waitForCacheClose();
        CloseableUtils.closeQuietly(client);
    }
    
    /* TODO 等待500ms, cache先关闭再关闭client, 否则会抛异常
     * 因为异步处理, 可能会导致client先关闭而cache还未关闭结束.
     * 等待Curator新版本解决这个bug.
     * BUG地址：https://issues.apache.org/jira/browse/CURATOR-157
     */
    private void waitForCacheClose() {
        try {
            Thread.sleep(500L);
        } catch (final InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
    }
    
    @Override
    public String get(final String key) {
        TreeCache cache = findTreeCache(key);

        //先从缓存获取
        if (null == cache) {
            return getDirectly(key);
        }
        ChildData resultInCache = cache.getCurrentData(key);
        if (null != resultInCache) {
            return null == resultInCache.getData() ? null : new String(resultInCache.getData(), Charsets.UTF_8);
        }
        return getDirectly(key);
    }

    /**
     * 从缓存中查找
     * @param key
     * @return
     */
    private TreeCache findTreeCache(final String key) {
        for (Entry<String, TreeCache> entry : caches.entrySet()) {
            if (key.startsWith(entry.getKey())) {
                return entry.getValue();
            }
        }
        return null;
    }

    /**
     * 直接从zk获得
     * @param key 键
     * @return
     */
    @Override
    public String getDirectly(final String key) {
        try {
            return new String(client.getData().forPath(key), Charsets.UTF_8);
        //CHECKSTYLE:OFF
        } catch (final Exception ex) {
        //CHECKSTYLE:ON
            RegExceptionHandler.handleException(ex);
            return null;
        }
    }
    
    @Override
    public List<String> getChildrenKeys(final String key) {
        try {
            List<String> result = client.getChildren().forPath(key);
            Collections.sort(result, new Comparator<String>() {
                
                @Override
                public int compare(final String o1, final String o2) {
                    return o2.compareTo(o1);
                }
            });
            return result;
         //CHECKSTYLE:OFF
        } catch (final Exception ex) {
        //CHECKSTYLE:ON
            RegExceptionHandler.handleException(ex);
            return Collections.emptyList();
        }
    }
    
    @Override
    public int getNumChildren(final String key) {
        try {
            Stat stat = client.checkExists().forPath(key);
            if (null != stat) {
                return stat.getNumChildren();
            }
            //CHECKSTYLE:OFF
        } catch (final Exception ex) {
            //CHECKSTYLE:ON
            RegExceptionHandler.handleException(ex);
        }
        return 0;
    }

    @Override
    public boolean isExisted(final String key) {
        try {
            return null != client.checkExists().forPath(key);
        //CHECKSTYLE:OFF
        } catch (final Exception ex) {
        //CHECKSTYLE:ON
            RegExceptionHandler.handleException(ex);
            return false;
        }
    }


    //存储持久节点数据。逻辑等价于 insertOrUpdate 操作
    @Override
    public void persist(final String key, final String value) {
        try {

            if (!isExisted(key)) {
                client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(key, value.getBytes(Charsets.UTF_8));
            } else {
                update(key, value);
            }
        //CHECKSTYLE:OFF
        } catch (final Exception ex) {
        //CHECKSTYLE:ON
            RegExceptionHandler.handleException(ex);
        }
    }
    
    @Override
    public void update(final String key, final String value) {
        try {
            //开启事务
            client.inTransaction().check().forPath(key).and().setData().forPath(key, value.getBytes(Charsets.UTF_8)).and().commit();
        //CHECKSTYLE:OFF
        } catch (final Exception ex) {
        //CHECKSTYLE:ON
            RegExceptionHandler.handleException(ex);
        }
    }
    
    @Override
    public void persistEphemeral(final String key, final String value) {
        try {
            //存储临时节点数据。节点类型无法变更，因此如果数据已存在，需要先进行删除
            if (isExisted(key)) {
                client.delete().deletingChildrenIfNeeded().forPath(key);
            }
            client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(key, value.getBytes(Charsets.UTF_8));
        //CHECKSTYLE:OFF
        } catch (final Exception ex) {
        //CHECKSTYLE:ON
            RegExceptionHandler.handleException(ex);
        }
    }
    
    @Override
    public String persistSequential(final String key, final String value) {
        try {
            return client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT_SEQUENTIAL).forPath(key, value.getBytes(Charsets.UTF_8));
        //CHECKSTYLE:OFF
        } catch (final Exception ex) {
        //CHECKSTYLE:ON
            RegExceptionHandler.handleException(ex);
        }
        return null;
    }
    
    @Override
    public void persistEphemeralSequential(final String key) {
        try {
            client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath(key);
        //CHECKSTYLE:OFF
        } catch (final Exception ex) {
        //CHECKSTYLE:ON
            RegExceptionHandler.handleException(ex);
        }
    }
    
    @Override
    public void remove(final String key) {
        try {
            //子节点数据也是一起删掉
            client.delete().deletingChildrenIfNeeded().forPath(key);
        //CHECKSTYLE:OFF
        } catch (final Exception ex) {
        //CHECKSTYLE:ON
            RegExceptionHandler.handleException(ex);
        }
    }

    /**
     * 获得该节点的最后更新时间( mtime )获得 Zookeeper 的时间
     * @param key 用于获取时间的键
     * @return
     */
    @Override
    public long getRegistryCenterTime(final String key) {
        long result = 0L;
        try {
            persist(key, "");
            result = client.checkExists().forPath(key).getMtime();
        //CHECKSTYLE:OFF
        } catch (final Exception ex) {
        //CHECKSTYLE:ON
            RegExceptionHandler.handleException(ex);
        }
        Preconditions.checkState(0L != result, "Cannot get registry center time.");
        return result;
    }
    
    @Override
    public Object getRawClient() {
        return client;
    }
    
    @Override
    public void addCacheData(final String cachePath) {
        //通过 Curator TreeCache 实现监控整个树( Zookeeper目录 )的数据订阅和缓存，包括节点的状态，子节点的状态
        TreeCache cache = new TreeCache(client, cachePath);
        try {
            cache.start();
        //CHECKSTYLE:OFF
        } catch (final Exception ex) {
        //CHECKSTYLE:ON
            RegExceptionHandler.handleException(ex);
        }
        caches.put(cachePath + "/", cache);
    }


    /**
     * @param cachePath 需释放缓存的路径
     */
    @Override
    public void evictCacheData(final String cachePath) {
        TreeCache cache = caches.remove(cachePath + "/");
        if (null != cache) {
            cache.close();
        }
    }
    
    @Override
    public Object getRawCache(final String cachePath) {
        return caches.get(cachePath + "/");
    }
}
