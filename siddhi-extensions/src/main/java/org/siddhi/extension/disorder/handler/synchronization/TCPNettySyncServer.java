/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *  WSO2 Inc. licenses this file to you under the Apache License,
 *  Version 2.0 (the "License"); you may not use this file except
 *  in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied. See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.siddhi.extension.disorder.handler.synchronization;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.apache.log4j.Logger;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * TCP Netty Server.
 */

public class TCPNettySyncServer {
    private static final Logger log = Logger.getLogger(TCPNettySyncServer.class);
    private ServerBootstrap bootstrap;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private ChannelFuture channelFuture;
    private String hostAndPort;
    private ServerConfig serverConfig;
    private FlowController flowController = new FlowController();
    private static TCPNettySyncServer instance = new TCPNettySyncServer();
    private AtomicInteger appsInUse = new AtomicInteger(0);
    private AtomicBoolean isStarted = new AtomicBoolean(false);

    private TCPNettySyncServer() {

    }

    public static TCPNettySyncServer getInstance() {
        return instance;
    }

    public synchronized void startIfNotAlready() {
        ServerConfig config = new ServerConfig();
        if (!isStarted.get()) {
            start(config);
            isStarted.set(true);
        }
        appsInUse.incrementAndGet();
    }


    private void start(ServerConfig serverConf) {
        serverConfig = serverConf;
        bossGroup = new NioEventLoopGroup(serverConfig.getReceiverThreads());
        workerGroup = new NioEventLoopGroup(serverConfig.getWorkerThreads());

        hostAndPort = serverConfig.getHost() + ":" + serverConfig.getPort();

        bootstrap = new ServerBootstrap();
        bootstrap.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer() {

                    @Override
                    protected void initChannel(Channel channel) throws Exception {
                        ChannelPipeline p = channel.pipeline();
                        p.addLast(new TCPServerInboundHandler());
                    }
                })
                .option(ChannelOption.TCP_NODELAY, true)
                .childOption(ChannelOption.SO_KEEPALIVE, true);

        try {
            // Bind and start to accept incoming connections.
            channelFuture = bootstrap.bind(serverConfig.getHost(), serverConfig.getPort()).sync();
            log.info("Time Sync Server started in " + hostAndPort + "");
        } catch (InterruptedException e) {
            log.error("Error when booting up tcp server on '" + hostAndPort + "' " + e.getMessage(), e);
        }
    }

    public void shutdownGracefullyIfNotUsed() {
        if (isStarted.get() && appsInUse.decrementAndGet() == 0) {
            channelFuture.channel().close();
            try {
                channelFuture.channel().closeFuture().sync();
            } catch (InterruptedException e) {
                log.error("Error when shutdowning the tcp server " + e.getMessage(), e);
            }
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
            log.info("Tcp Server running on '" + hostAndPort + "' stopped.");
            workerGroup = null;
            bossGroup = null;
        }
    }

    public void pause() {
        flowController.pause();
    }

    public void resume() {
        flowController.resume();
    }
}

