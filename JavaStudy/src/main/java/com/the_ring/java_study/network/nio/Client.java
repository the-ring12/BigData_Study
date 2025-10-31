package com.the_ring.java_study.network.nio;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Scanner;

/**
 * @Description TODO
 * @Date 2025/10/30
 * @Author the_ring
 */
public class Client {
    public static void main(String[] args) throws IOException {
        // 1. 获取通道
        SocketChannel socketChannel = SocketChannel.open(new InetSocketAddress("127.0.0.1", 9999));
        // 2. 设置为非阻塞
        socketChannel.configureBlocking(false);
        // 3. 分配指定缓冲区
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        // 4. 发送数据给服务端
        Scanner scanner = new Scanner(System.in);
        while (true) {
            System.out.print("请说：");
            String msg = scanner.nextLine();
            buffer.put(msg.getBytes());
            buffer.flip();
            socketChannel.write(buffer);
            buffer.clear();
        }
    }
}
