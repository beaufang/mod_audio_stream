# 播放与会话清理修复验证

修复基线：`8be8361`。本地回退 tag：`pre-safety-fix-20260909`。
修改位于原分支 `feature/bi_stream`，未推送或部署。

## 关键变更

- 会话发布前，由清理线程取得 session read lock；同一线程在播放线程 join、WebSocket 回调退出以及资源释放后解锁。避免跨线程释放 rwlock，也不再依赖定时器或 session pool 的延迟回收时间。
- 清理任务在初始化时预留。挂断回调只发出关闭信号并提交任务，不创建新的原生线程，不在回调持有的锁内 join。线程创建失败和初始化失败也走同一释放路径。
- 播放线程关闭各自的 WebSocket，多个呼叫可并行完成 close handshake；独立 drain 线程负责 join，引用所有者线程负责最终释放。活跃会话持模块 rwlock，阻止普通卸载进入接口移除阶段。
- 共享关闭状态改为原子访问；保留 pool mutex 到 session 销毁，保护已取得旧状态指针的 API 调用。队列和重采样器操作增加异常安全的解锁；WebSocket 客户端的发送与断开通过互斥锁和所有权转移隔离。
- 删除固定 PCMA/PT 8 的 RTP 直写以及手写 A-law 编码，恢复 FreeSWITCH 的 L16 → 协商 codec → Channel 路径。尾部不足一帧时补静音，避免最后一个短片段滞留在核心缓冲区。删除了直接 RTP 返回 0 仍判定成功的路径。
- flush 默认恢复原有兼容语义：不等待取消响应的 mark，避免后续整句无声。`STREAM_FLUSH_WAIT_FOR_MARK=true` 显式启用严格屏障。generation 和回执前检查继续阻止本地在途工作越过 flush。

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
- 默认 flush 不再要求取消响应的终止 mark；发送端必须停止发送取消响应。无 responseId 时无法严格识别网络迟到的旧 PCM/mark。只有显式启用严格屏障时，才要求旧 mark 在 flush 之后、新 PCM 之前到达。
- 真实通话中交叉运行 flush、stop、快速挂断、连接错误和反复 start/stop；观察接收端音频和 mark 顺序。stop 的 `+OK` 只表示接受关闭请求，后台清理仍可能在执行。
- 长时间呼叫循环后观察 RSS、虚拟内存、文件描述符和线程资源，并在兼容环境运行 TSan。单元测试通过不代表完整 FreeSWITCH 进程已通过生产稳定性验证。

## 无声回归修正

用户报告第一句正常、后续无声。缺少本次通话日志，不能宣称已确定现场根因。
真实队列测试复现：播放第一句 → 连续 flush → 取消响应不发 mark → 下一句 PCM 被丢弃。
修复前失败信息：`missing cancelled mark must not silence the next response`。
修复默认 flush 后该路径通过；旧的严格屏障测试使用显式参数，仍保持迟到音频隔离验证。
宿主测试直接调用 `stream_session_flush`，覆盖缺省变量和显式严格模式。
逐条 streamAudio 接收日志从 INFO 改为 DEBUG。

清理执行器增加独立 drain 线程，阻塞的 writer join/WebSocket disconnect 与 session 引用获取、最终释放分离。
会话引用仍在同一个线程获取和释放。慢 join 仍可能积压后续清理，但不会再占用处理 start 的线程。
拒绝 shutdown 不再关闭任务入口。活跃会话在同一线程持有模块 rwlock，普通 unload 在 FreeSWITCH
`do_shutdown` 的 trywrlock 阶段即失败，避免先移除 API 再返回 NOUNLOAD。强制卸载会绕过该保护，
不属于支持的活跃通话操作。join 失败仍保留 session/module 锁，避免 UAF。
宿主测试使用真实 pthread rwlock 验证活跃时写锁失败、清理后写锁成功，并验证一个 writer 卡住时
另一个实际 session reserve 能完成。该测试仍非完整 FreeSWITCH 加载器集成测试。
