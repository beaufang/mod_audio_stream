# 播放与会话清理修复验证

修复基线：`8be8361`。本地回退 tag：`pre-safety-fix-20260909`。
修改位于原分支 `feature/bi_stream`，未推送或部署。

## 关键变更

- 会话发布前，由清理线程取得 session read lock；同一线程在播放线程 join、WebSocket 回调退出以及资源释放后解锁。避免跨线程释放 rwlock，也不再依赖定时器或 session pool 的延迟回收时间。
- 清理任务在初始化时预留。挂断回调只发出关闭信号并提交任务，不创建新的原生线程，不在回调持有的锁内 join。线程创建失败和初始化失败也走同一释放路径。
- 播放线程关闭各自的 WebSocket，多个呼叫可并行完成 close handshake；后台回收线程负责 join 和最终释放。模块在存在未完成会话时不卸载代码。
- 共享关闭状态改为原子访问；保留 pool mutex 到 session 销毁，保护已取得旧状态指针的 API 调用。队列和重采样器操作增加异常安全的解锁；WebSocket 客户端的发送与断开通过互斥锁和所有权转移隔离。
- 删除固定 PCMA/PT 8 的 RTP 直写以及手写 A-law 编码，恢复 FreeSWITCH 的 L16 → 协商 codec → Channel 路径。尾部不足一帧时补静音，避免最后一个短片段滞留在核心缓冲区。删除了直接 RTP 返回 0 仍判定成功的路径。
- 恢复 flush 的旧响应 mark 屏障；generation 只用于本地在途处理。mark 回执发送前重新检查 generation，防止已经取出的 mark 越过 flush。

## 已执行

1. 修改前执行原队列测试，稳定失败：`flush before the marker should open the late-audio discard window`。
2. 修改后执行队列测试，覆盖迟到 PCM、连续 flush、旧 mark 作废、下一句恢复、容量和 mark 数量上限。
3. 清理执行器测试覆盖发布前获取引用、延迟 writer、在 join 完成前保持资源、锁的线程归属、创建失败、关闭后拒绝新任务和四个生产线程共 1,000 次任务的恰好一次执行。
4. `session_cleanup_test` 直接编译并调用实际模块的挂断、abort 和最终清理函数，替换少量 FreeSWITCH 宿主 API。验证挂断回调不递归移除 bug、不等待 writer，重复停止不重复释放，writer 退出前保留 queue/session，join 后释放，以及无 writer 的初始化失败路径。
5. 使用本地 FreeSWITCH 源码头文件和 Ubuntu SpeexDSP 1.2.1 开发头文件完成 C/C++ 语法检查。FreeSWITCH 的生成配置头在临时目录按当前 Linux 64 位平台生成；这不是部署环境的完整链接构建。
6. 单元测试执行 ASan、UBSan 和 LeakSanitizer。沙箱的 ptrace 会阻止 LeakSanitizer，实际检查在沙箱外完成。

ThreadSanitizer 在当前宿主环境启动失败：`FATAL: ThreadSanitizer: unexpected memory mapping`。不能据此声称已通过 TSan。

## 复现命令

有完整开发环境时：

```sh
FREESWITCH_CFLAGS="$(pkg-config --cflags freeswitch speexdsp)" bash tests/run-unit-tests.sh
FREESWITCH_CFLAGS="$(pkg-config --cflags freeswitch speexdsp)" SANITIZERS=address,undefined bash tests/run-unit-tests.sh
cmake -S . -B build -DBUILD_TESTING=ON -DCMAKE_BUILD_TYPE=Debug
cmake --build build -j
ctest --test-dir build --output-on-failure
```

没有 FreeSWITCH 开发文件时，直接运行脚本仍会验证 queue 和 cleanup executor，并明确跳过宿主清理测试。

## 上线前仍需完成

- 在实际部署的 FreeSWITCH 开发环境完成模块链接和加载，核对二进制与源码/SDP 行为。当前机器没有完整 FreeSWITCH 安装、CMake 和 pkg-config；未完成可部署 `.so` 的构建。
- PCMA、PCMU、Opus 等实际使用的 codec，分别验证 PCM 下行、短尾帧、不同包时长和采样率。确认合法媒体方向下标准 Channel 写入可听到声音，hold/unhold 后恢复正常。
- TTS 服务必须在取消旧响应后仍发送它的终止 mark，并且先于新响应 PCM。否则恢复屏障会继续丢弃音频，这是当前无 responseId 协议的必要约束。
- 真实通话中交叉运行 flush、stop、快速挂断、连接错误和反复 start/stop；观察接收端音频和 mark 顺序。stop 的 `+OK` 只表示接受关闭请求，后台清理仍可能在执行。
- 长时间呼叫循环后观察 RSS、虚拟内存、文件描述符和线程资源，并在兼容环境运行 TSan。单元测试通过不代表完整 FreeSWITCH 进程已通过生产稳定性验证。
