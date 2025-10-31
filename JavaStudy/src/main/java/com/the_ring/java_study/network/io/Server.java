package com.the_ring.java_study.network.io;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * @Description TCP 通信的实现，三次握手和四次挥手
 * @Date 2025/10/29
 * @Author the_ring
 */
public class Server {

    public static void main(String[] args) {
        ServerSocket serverSocket = null;
        Socket clientSocket = null;
        InputStream in = null;
        OutputStream out = null;
        try {
            // 创建ServerSocket，监听指定端口
            serverSocket = new ServerSocket(8888);
            System.out.println("服务器启动，等待客户端连接...");
            // 等待客户端连接，accept()方法会阻塞，直到有客户端连接
            // 这里隐含了三次握手的过程
            clientSocket = serverSocket.accept();
            System.out.println("客户端连接成功，客户端地址：" + clientSocket.getInetAddress());
            // 获取输入流和输出流
            in = clientSocket.getInputStream();
            out = clientSocket.getOutputStream();
            // 读取客户端发送的数据
            byte[] buffer = new byte[1024];
            int len = in.read(buffer);
            System.out.println("收到客户端数据：" + new String(buffer, 0, len));
            // 向客户端发送数据
            String response = "Hello, Client!";
            out.write(response.getBytes());
            out.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                // 关闭资源，隐含了四次挥手的过程
                if (in != null)
                    in.close();
                if (out != null)
                    out.close();
                if (clientSocket != null)
                    clientSocket.close();
                if (serverSocket != null)
                    serverSocket.close();
                System.out.println("服务器连接关闭");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
