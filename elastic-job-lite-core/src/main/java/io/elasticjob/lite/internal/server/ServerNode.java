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

package io.elasticjob.lite.internal.server;

import io.elasticjob.lite.internal.schedule.JobRegistry;
import io.elasticjob.lite.internal.storage.JobNodePath;
import io.elasticjob.lite.util.env.IpUtils;

import java.util.regex.Pattern;

/**
 * 服务器节点路径.
 * [zk: localhost:2181(CONNECTED) 72] ls /elastic-job-example-lite-java/javaSimpleJob/servers
 * [192.168.16.164, 169.254.93.156, 192.168.252.57, 192.168.16.137, 192.168.3.2, 192.168.43.31]
 *
 * /servers/ 目录下以 IP 为数据节点路径存储每个服务器节点。如果相同IP服务器有多个服务器节点，只存储一个 IP 数据节点
 * 是持久节点，不存储任何信息，只是空串( "")
 * 
 * @author zhangliang
 */
public final class ServerNode {
    
    /**
     * 服务器信息根节点.
     */
    public static final String ROOT = "servers";
    
    private static final String SERVERS = ROOT + "/%s";
    
    private final String jobName;
    
    private final JobNodePath jobNodePath;
    
    public ServerNode(final String jobName) {
        this.jobName = jobName;
        jobNodePath = new JobNodePath(jobName);
    }
    
    /**
     * 判断给定路径是否为作业服务器路径.
     *
     * @param path 待判断的路径
     * @return 是否为作业服务器路径
     */
    public boolean isServerPath(final String path) {
        return Pattern.compile(jobNodePath.getFullPath(ServerNode.ROOT) + "/" + IpUtils.IP_REGEX).matcher(path).matches();
    }
    
    /**
     * 判断给定路径是否为本地作业服务器路径.
     *
     * @param path 待判断的路径
     * @return 是否为本地作业服务器路径
     */
    public boolean isLocalServerPath(final String path) {
        return path.equals(jobNodePath.getFullPath(String.format(SERVERS, JobRegistry.getInstance().getJobInstance(jobName).getIp())));
    }
    
    String getServerNode(final String ip) {
        return String.format(SERVERS, ip);
    }
}