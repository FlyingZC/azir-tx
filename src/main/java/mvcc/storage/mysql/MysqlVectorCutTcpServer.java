package mvcc.storage.mysql;

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/** 回环二进制切面端点以单次写出长度与正文，避免 HTTP 头与正文分开发送。 */
final class MysqlVectorCutTcpServer implements AutoCloseable {
    private final MysqlVectorCutLeaderRuntime runtime;
    private final ServerSocket listener;
    private final ExecutorService requests = Executors.newVirtualThreadPerTaskExecutor();
    private final AtomicBoolean running = new AtomicBoolean();
    private volatile Thread acceptor;

    /** 只绑定本机回环地址，监听端口须与中心租约的 ownerId 相同。 */
    MysqlVectorCutTcpServer(MysqlVectorCutLeaderRuntime runtime, int port) throws IOException {
        this.runtime = Objects.requireNonNull(runtime, "runtime");
        if (port < 0 || port > 65535) {
            throw new IllegalArgumentException("invalid vector cut TCP port");
        }
        listener = new ServerSocket();
        listener.setReuseAddress(true);
        listener.bind(new InetSocketAddress("127.0.0.1", port), 32);
    }

    /** 取得系统分配的实际回环端口。 */
    int port() {
        return listener.getLocalPort();
    }

    /** 只有发布运行时已取得有效租约，才开始接收远端切面请求。 */
    void start() {
        if (!runtime.isServing() || !running.compareAndSet(false, true)) {
            throw new IllegalStateException("vector cut TCP endpoint requires an active leader");
        }
        acceptor = Thread.ofVirtual().name("azir-vector-cut-tcp-accept").start(this::acceptLoop);
    }

    /** 每次复用连接仍重新验证领导者资格，避免高频 RC 读耗尽临时端口。 */
    private void acceptLoop() {
        while (running.get()) {
            try {
                Socket socket = listener.accept();
                requests.submit(() -> serveOne(socket));
            } catch (SocketException ex) {
                if (running.get()) {
                    runtime.endpointUnavailable(ex);
                    running.set(false);
                }
            } catch (IOException ex) {
                if (running.get()) {
                    runtime.endpointUnavailable(ex);
                    running.set(false);
                }
            } catch (RuntimeException ex) {
                if (running.get()) {
                    runtime.endpointUnavailable(ex);
                    running.set(false);
                }
            }
        }
    }

    /** 每个请求码后都返回有符号长度与完整切面，失去租约则回错误并关闭连接。 */
    private void serveOne(Socket socket) {
        try (socket;
             var input = new DataInputStream(socket.getInputStream());
             var output = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()))) {
            socket.setTcpNoDelay(true);
            socket.setSoTimeout(2_000);
            while (running.get()) {
                int command;
                try {
                    command = input.readUnsignedByte();
                } catch (EOFException ex) {
                    break;
                }
                if (command != 1) {
                    output.writeInt(-2);
                    output.flush();
                    break;
                }
                try {
                    byte[] body = MysqlVectorCutWire.encode(runtime.currentView());
                    output.writeInt(body.length);
                    output.write(body);
                    output.flush();
                } catch (RuntimeException ex) {
                    output.writeInt(-1);
                    output.flush();
                    break;
                }
            }
        } catch (IOException ignored) {
            // 对端断开只影响本次只读请求，不改变领导者租约或发布状态。
        }
    }

    /** 停止监听并回收请求线程，旧领导者仍由运行时租约栅栏拒绝。 */
    @Override
    public void close() throws Exception {
        runtime.close();
        running.set(false);
        listener.close();
        Thread activeAcceptor = acceptor;
        if (activeAcceptor != null) {
            activeAcceptor.interrupt();
            activeAcceptor.join(5_000);
        }
        requests.close();
    }
}
