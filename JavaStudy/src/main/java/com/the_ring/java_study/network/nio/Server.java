package com.the_ring.java_study.network.nio;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.Iterator;

/**
 * @Description TODO
 * @Date 2025/10/30
 * @Author the_ring
 */
public class Server {

    public static void main(String[] args) throws IOException {
        // 1. 获取通道
        ServerSocketChannel serverChannel = ServerSocketChannel.open();
        // 2. 切换为非阻塞
        serverChannel.configureBlocking(false);
        // 3. 绑定连接端口
        serverChannel.bind(new InetSocketAddress(9999));
        // 4. 获取选择器
        Selector selector = Selector.open();
        // 5. 将通道注册到选择器上，并切开始监听接收事件
        serverChannel.register(selector, SelectionKey.OP_ACCEPT);
        // 6. 使用 Selector 选择器轮询已经就绪好的事件
        while (selector.select() > 0) {
            // 7. 获取选择器中所有注册的通道中已经就绪好的事件
            Iterator<SelectionKey> iterator = selector.selectedKeys().iterator();
            // 8. 开始遍历好的事件
            while (iterator.hasNext()) {
                SelectionKey selectionKey = iterator.next();
                // 9. 判断事件具体是什么
                if (selectionKey.isAcceptable()) {
                    // 获取客户端通道
                    SocketChannel acceptChannel = serverChannel.accept();
                    // 设置为非阻塞
                    acceptChannel.configureBlocking(false);
                    // 将客户端彩通道注册到选择器
                    acceptChannel.register(selector, SelectionKey.OP_READ);
                } else if (selectionKey.isReadable()) {
                    // 获取通道
                    SocketChannel socketChannel = (SocketChannel) selectionKey.channel();
                    // 读取数据
                    ByteBuffer buffer = ByteBuffer.allocate(1024);
                    int len = 0;
                    while ((len = socketChannel.read(buffer)) > 0) {
                        buffer.flip();
                        System.out.println(new String(buffer.array(), 0, len));
                        buffer.clear();
                    }
                }

                iterator.remove();
            }
        }
    }
}
