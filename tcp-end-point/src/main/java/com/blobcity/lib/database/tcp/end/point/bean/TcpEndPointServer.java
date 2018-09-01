/**
 * Copyright (C) 2018 BlobCity Inc
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published
 * by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.blobcity.lib.database.tcp.end.point.bean;

import com.blobcity.lib.database.bean.manager.common.Constants;
import com.blobcity.lib.database.bean.manager.interfaces.Stoppable;
import com.blobcity.lib.database.launcher.util.ConfigUtil;
import com.blobcity.lib.database.tcp.end.point.decoder.TcpStreamDecoder;
import com.blobcity.lib.database.tcp.end.point.handler.TcpStreamHandler;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import java.util.List;
import java.util.Map;
import javax.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Bean that starts the TCP end point for the database
 *
 * @author javatarz (Karun Japhet)
 */
@Component
public class TcpEndPointServer implements Stoppable {

    private static final Logger logger = LoggerFactory.getLogger(TcpEndPointServer.class);
    private TcpServer tcpServer;

    @PostConstruct
    private void init() {
        final Map<String, List<String>> portConfigData = ConfigUtil.getConfigData(Constants.APP_CONFIG_FILE, "/config/ports/port");
        final int port = Integer.parseInt(portConfigData.get(this.getClass().getName()).get(0));

        tcpServer = new TcpServer("TCP-EP", port);
        tcpServer.start();
    }

    @Override
    public void stop() {
        if (tcpServer != null) {
            tcpServer.shutdownGracefully();
        } else {
            logger.warn("TCP End Point stop requested but the server seems to have not been started yet");
        }
    }

    /**
     * Class to manage the TCP Server thread since the server internally holds the thread after starting
     *
     * @author Karun AB <karun.ab@blobcity.net>
     */
    private class TcpServer extends Thread {

        private EventLoopGroup bossGroup;
        private EventLoopGroup workerGroup;
        private final int port;

        /**
         * Instantiates an instance of a TCP server thread
         *
         * @param threadName Name of the thread
         * @param port port on which the server is to be started
         */
        public TcpServer(final String threadName, final int port) {
            super(threadName);
            this.port = port;
        }

        @Override
        public void run() {
            bossGroup = new NioEventLoopGroup();
            workerGroup = new NioEventLoopGroup();
            try {
                final ServerBootstrap bootstrap = new ServerBootstrap();
                bootstrap.group(bossGroup, workerGroup)
                        .channel(NioServerSocketChannel.class)
                        .childHandler(new ChannelInitializer<SocketChannel>() {
                            @Override
                            public void initChannel(SocketChannel ch) throws Exception {
                                final TcpStreamDecoder decoder = new TcpStreamDecoder();
                                final TcpStreamHandler handler = new TcpStreamHandler(decoder);

                                ch.pipeline().addLast(decoder, handler);
                            }
                        })
                        .option(ChannelOption.SO_BACKLOG, 128)
                        .childOption(ChannelOption.SO_KEEPALIVE, true);

                // Bind and start to accept incoming connections.
                final ChannelFuture f = bootstrap.bind(port).sync();
                logger.info("TCP end point successfully started at port {}", port);

                // Wait until the server socket is closed.
                // In this example, this does not happen, but you can do that to gracefully
                // shut down your server.
                f.channel().closeFuture().sync();
            } catch (InterruptedException ex) {
                logger.error("TCP Server action was interrupted", ex);
            } finally {
                workerGroup.shutdownGracefully();
                bossGroup.shutdownGracefully();
            }
        }

        /**
         * Stops the TCP server when a shut down is triggered by requesting graceful shutdown where possible
         */
        public void shutdownGracefully() {
            final boolean requestWorkerGroupStop = workerGroup != null;
            final boolean requestBossGroupStop = bossGroup != null;

            if (requestWorkerGroupStop) {
                workerGroup.shutdownGracefully();
            }

            if (requestBossGroupStop) {
                bossGroup.shutdownGracefully();
            }

            logger.info("TCP End Point stop requested. Group request status => [Worker: {}, Boss: {}]", requestWorkerGroupStop, requestBossGroupStop);
        }
    }
}
