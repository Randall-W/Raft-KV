#include <iostream>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <cstring>
#include <string>

std::string build_redis_command(const std::string& command, const std::string& key, const std::string& value = "") {
    if (command == "SET") {
        return "*3\r\n$3\r\nSET\r\n$" + std::to_string(key.length()) + "\r\n" + key + "\r\n$" + std::to_string(value.length()) + "\r\n" + value + "\r\n";
    } else if (command == "GET") {
        return "*2\r\n$3\r\nGET\r\n$" + std::to_string(key.length()) + "\r\n" + key + "\r\n";
    } else if (command == "DEL") {
        return "*2\r\n$3\r\nDEL\r\n$" + std::to_string(key.length()) + "\r\n" + key + "\r\n";
    }
    return "";
}


int main() {
    const char* server_ip = "127.0.0.1"; // Redis 服务器 IP 地址
    int server_port = 8084;              // Redis 服务器端口

    // 创建 socket
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock == -1) {
        std::cerr << "Could not create socket" << std::endl;
        return 1;
    }

    // 设置服务器地址
    sockaddr_in server_address;
    server_address.sin_family = AF_INET;
    server_address.sin_port = htons(server_port);
    if (inet_pton(AF_INET, server_ip, &server_address.sin_addr) <= 0) {
        std::cerr << "Invalid address/ Address not supported" << std::endl;
        close(sock);
        return 1;
    }

    // 连接到服务器
    if (connect(sock, (struct sockaddr*)&server_address, sizeof(server_address)) < 0) {
        std::cerr << "Connection failed" << std::endl;
        close(sock);
        return 1;
    }

    while (true) {
        std::string command, key, value;
        std::cout << "Enter command (SET/GET/DEL/exit): ";
        std::cin >> command;

        if (command == "exit") {
            break;
        }

        std::cout << "Enter key: ";
        std::cin >> key;

        if (command == "SET") {
            std::cout << "Enter value: ";
            std::cin >> value;
        }

        std::string redis_command = build_redis_command(command, key, value);
        if (redis_command.empty()) {
            std::cerr << "Invalid command" << std::endl;
            continue;
        }

        // 发送命令到 Redis 服务器
        if (send(sock, redis_command.c_str(), redis_command.length(), 0) != redis_command.length()) {
            std::cerr << "Send failed" << std::endl;
            continue;
        }

        // 接收 Redis 服务器的响应
        char buffer[1024] = {0};
        int bytes_received = recv(sock, buffer, sizeof(buffer) - 1, 0);
        if (bytes_received < 0) {
            std::cerr << "Receive failed" << std::endl;
        } else {
            buffer[bytes_received] = '\0';
            std::cout << "Response from server: " << buffer << std::endl;
        }
    }

    // 关闭 socket
    close(sock);

    return 0;
}
