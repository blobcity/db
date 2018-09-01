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

package com.blobcity.lib.database.tcp.end.point.handler;

import com.blobcity.lib.database.bean.manager.factory.BeanConfigFactory;
import com.blobcity.lib.database.bean.manager.interfaces.engine.RequestStore;
import com.blobcity.lib.database.bean.manager.interfaces.engine.SqlExecutor;
import com.blobcity.lib.database.tcp.end.point.decoder.TcpStreamDecoder;
import com.blobcity.lib.database.tcp.end.point.decoder.packet.LoginRequestPacket;
import com.blobcity.lib.database.tcp.end.point.decoder.packet.base.Packet;
import com.blobcity.lib.database.tcp.end.point.decoder.packet.SqlQueryPacket;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;

/**
 * Handles the stream of data that has been decoded by a channel decoder
 *
 * @author javatarz (Karun Japhet)
 */
public class TcpStreamHandler extends ChannelInboundHandlerAdapter {

    private final SqlExecutor sqlExecutorBean;
    private final RequestStore requestStore;
    private final Logger logger;
    private final TcpStreamDecoder decoder;
    private LoginRequestPacket sessionBean;

    /**
     * Creates an instance of channel stream that is associated with the respective decoder
     *
     * @param decoder upstream decoder that decodes {@link ByteBuff}s into parsed consumable {@link Object}s
     */
    public TcpStreamHandler(final TcpStreamDecoder decoder) {
        this.logger = LoggerFactory.getLogger(TcpStreamHandler.class.getName() + ":" + System.currentTimeMillis());
        ApplicationContext context = BeanConfigFactory.getConfigBean("com.blobcity.pom.database.engine.factory.EngineBeanConfig");
        this.sqlExecutorBean = context.getBean(SqlExecutor.class);
        this.requestStore = context.getBean(RequestStore.class);
        this.decoder = decoder;
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, final Object msg) {
        try {
            if (msg instanceof Exception) {
                logger.error("Something broke. Write an appropriate message in the channel", (Exception) msg); // TODO: Handle
            } else if (msg instanceof Packet) {
                logger.debug("Found message: \"{}\"", msg);

                final String response = processPacket((Packet) msg);

                final byte[] bytes = response.getBytes();
                final ByteBuf respBytes = ctx.alloc().buffer(bytes.length);
                respBytes.writeBytes(bytes, 0, bytes.length);
                ctx.writeAndFlush(respBytes); // Note: this call is async afaik
            } else {
                logger.error("Unknown message found of type \"{}\" and value \"{}\"", msg.getClass(), msg); // TODO: Handle
            }
        } catch (Throwable t) {
            logger.error("Uncaught exception found while handling TCP stream", t);
            // TODO: handle errors correctly
        }
    }

    /**
     * Processes packets based on the packet type and provides a response to be returned to the client
     *
     * @param packet {@link Packet} that has been created by the upstream decoder that now has to be processed based on the request
     * @return response to be sent to the calling client based on the request
     */
    public String processPacket(final Packet packet) {
        logger.trace("Processing packet: {}", packet);

        switch (packet.getHeader().getType()) {
            case SQL_BATCH:
                return processPacket((SqlQueryPacket) packet);
            case LOGIN_REQ:
                return processPacket((LoginRequestPacket) packet);
            default:
                throw new UnsupportedOperationException("Header type " + packet.getHeader().getType() + " is not yet supported.");
        }
    }

    /**
     * Processes SQL packets by invoking the appropriate database bean
     *
     * @param sqlPacket {@link Packet} containing SQL query data
     * @return response to the requested query
     */
    public String processPacket(final SqlQueryPacket sqlPacket) {
        if (sessionBean == null) {
            logger.error("No session bean found!");
            return "No session bean found!"; // TODO: Write error response here
        }

        //TODO: If secure then change sessionBean to store password instead of passwordHash, or change everything else to work on passwordHash only
        final String requestId = requestStore.registerNewRequest(sessionBean.getDbName(), sessionBean.getUserName(),sessionBean.getPasswordHash(), null);
        final String response = sqlExecutorBean.runQuery(requestId, sessionBean.getUserName(), sessionBean.getPasswordHash(), sessionBean.getDbName(), sqlPacket.getData());
        logger.debug("[App: {} - SQL: {}] executed. Received a response length of {}.", sessionBean.getDbName(), sqlPacket.getData(), response.length());
        requestStore.unregisterRequest(requestId);
        return response;
    }

    /**
     * Processes Login Request packets
     *
     * @param loginPacket {@link Packet} containing login data
     * @return response to be sent for the login request
     */
    public String processPacket(final LoginRequestPacket loginPacket) {
        // TODO: perform login check here
        this.sessionBean = loginPacket;

        logger.debug("Login successful for user name {}", loginPacket.getUserName());
        return "Login Successful"; // TODO: Write login response here
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) { // (4)
        cause.printStackTrace();
        ctx.close(); // Make closing situational through new parent exception types
    }
}
