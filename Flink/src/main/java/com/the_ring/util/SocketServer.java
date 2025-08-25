package com.the_ring.util;

import java.awt.*;
import java.awt.event.KeyEvent;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Scanner;

/**
 * @Description 代替 nc 工具实现数据流
 * @Date 2025/8/25
 * @Author the_ring
 */
public class SocketServer {

    public static void main(String[] args) throws AWTException {
        Scanner scanner = new Scanner(System.in);
        final int port = 9090;
        byte[] buffer = new byte[1024];
        final boolean[] isConnected = {false};

        // 模拟输入，即使终止本地输入
        Robot robot = new Robot();

        try (ServerSocket serverSocket = new ServerSocket(port)) {
            System.out.println("Server listening on port " + port);

            while (true) {
                try (Socket socket = serverSocket.accept();
                     PrintStream out = new PrintStream(socket.getOutputStream(), true);
                     InputStream in = socket.getInputStream()
                ) {
                    System.out.println("Already establish a connection, you can send messages now, edit 'e!' to exit!!");
                    isConnected[0] = true;

                    // start  monitor thread to monit connected status
                    new Thread(() -> {
                        try {
                            if (in.read(buffer) == -1) {
                                System.out.println("Connection is closed");
                                isConnected[0] = false;

                                // 模拟按下回车键
                                robot.keyPress(KeyEvent.VK_ENTER);
                                robot.keyRelease(KeyEvent.VK_ENTER);
                            }
                        } catch (IOException e) {
                            System.out.println("Connection is closed");
                            isConnected[0] = false;

                            // 模拟按下回车键
                            robot.keyPress(KeyEvent.VK_ENTER);
                            robot.keyRelease(KeyEvent.VK_ENTER);
                        }
                    }).start();


                    while (isConnected[0]) {
                        // 这里存在一个问题，即使已经检测到对方掉线，会阻塞在接收输入，需要使用非阻塞输入才能实现及时终止这个过程
                        // 这里检测到对方退出时，模拟一个回车输入，让它退出这个阻塞
                        String message = scanner.nextLine();
                        if ("e!".equals(message)) {
                            System.out.println("closing the connection!");
                            isConnected[0] = false;
                            break;
                        }
                        out.println(message);
                    }
                } catch (IOException e) {
                    System.out.println("v");
                    throw new RuntimeException(e);
                }
            }
        } catch (IOException e) {
            System.out.println("d");
            throw new RuntimeException(e);
        }
    }
}

