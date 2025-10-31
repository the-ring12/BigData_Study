package com.the_ring.java_study.network.io;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

/**
 * @Description TODO
 * @Date 2025/10/29
 * @Author the_ring
 */
public class Client {
    public static void main(String[] args) {
        Socket socket = null;
        OutputStream out = null;
        InputStream in = null;
        try {
            // 创建Socket，连接服务器，这里隐含了三次握手的过程
            socket = new Socket("127.0.0.1", 8888);
            System.out.println("连接服务器成功");
            // 获取输入流和输出流
            out = socket.getOutputStream();
            in = socket.getInputStream();
            // 向服务器发送数据
            String data = "Hello, Server!";
            out.write(data.getBytes());
            out.flush();
            // 读取服务器返回的数据
            byte[] buffer = new byte[1024];
            int len = in.read(buffer);
            System.out.println("收到服务器数据：" + new String(buffer, 0, len));
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                // 关闭资源，隐含了四次挥手的过程
                if (out != null)
                    out.close();
                if (in != null)
                    in.close();
                if (socket != null)
                    socket.close();
                System.out.println("客户端连接关闭");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
