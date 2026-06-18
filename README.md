# FT300S

FT300S 是一个力/力矩采集服务，整体设计参考了 XenseTacSensor 的服务架构。

默认运行配置已针对 FT300 的 stream 模式优化到 100Hz。

## 包含内容

- FT300 传感器客户端（支持 Modbus RTU 读取，支持可选 stream 模式）
- UDS 控制通道（消息模型与 XenseTacSensor 保持一致）
- 共享内存写入器（v2 双缓冲 latest-index 协议）
- 本地内存缓存与按 demo 粒度的 numpy 落盘
- UDS 联调客户端与 SHM 读取测试客户端
- UDS + SHM 一体化联动（在同一个 UDS 客户端内启动/停止 SHM reader）
- 100Hz 在线基准测试脚本（CSV 原始数据 + JSON 汇总）

## 目录说明

- app.py：服务入口
- config/settings.py：运行参数配置
- core/service.py：采集主循环与状态机动作
- core/state.py：生命周期状态迁移
- io/sensor_client.py：FT300 硬件访问与数据转换
- io/shm_writer.py：v2 共享内存写入器
- io/uds_channel.py：Unix Socket 通道封装
- io/local_store.py：内存缓存与 npy 落盘
- protocol/messages.py：消息协议定义与编解码
- shm_read_test_client.py：最小 SHM 联调读取端
- uds_test_client.py：最小 UDS 联调客户端
- benchmark_100hz.py：100Hz 在线基准测试工具

## 启动服务

在工作区根目录执行：

```bash
python -m FT300S.app --uds-path /tmp/ft300_sensor.sock --shm-name ft300_sensor_frame --fps 100
```

可选参数：

- --save-dir：采集文件保存目录（默认仓库根目录下的 runtime_frames）
- --port：串口路径（默认使用 /dev/serial/by-id/...）
- --slave-address：Modbus 从站地址（默认 9）
- --stream-mode：强制使用 stream 模式（默认行为）
- --modbus-mode：切换为 Modbus 寄存器模式（回退方案）

## 启动 UDS 客户端

```bash
python -m FT300S.uds_test_client --uds-path /tmp/ft300_sensor.sock
```

## UDS + SHM 一体联调（推荐）

该模式下，UDS 客户端会在 START_REQ / DEMO_DONE_REQ 等关键命令点，自动联动 SHM reader，便于验证“服务写入 100Hz -> SHM reader 读取”的闭环。

```bash
python -m FT300S.uds_test_client \
	--uds-path /tmp/ft300_sensor.sock \
	--with-shm-reader \
	--shm-name ft300_sensor_frame \
	--reader-target-hz 100
```

常用联动参数：

- --with-shm-reader：启用 SHM 联动读取器
- --shm-name：共享内存名称
- --reader-target-hz：SHM reader 读取频率（建议与服务 fps 一致）
- --reader-max-retries：单次读取最大重试次数
- --ack-timeout：等待 ACK 超时
- --init-timeout：等待 INIT_READY 超时
- --done-stop-delay-ms：发送 DEMO_DONE_REQ 后延时停止 reader，用于覆盖收尾窗口

交互命令键：

- i：发送 INIT_REQ
- s：发送 START_REQ
- p：发送 PAUSE_REQ
- d：发送 DEMO_DONE_REQ
- x：发送 DEMO_DISCARD_REQ
- q：发送 STOP_REQ 并退出

脚本模式示例：

```bash
python -m FT300S.uds_test_client \
	--uds-path /tmp/ft300_sensor.sock \
	--with-shm-reader \
	--script "s,wait:5,d,q"
```

## 启动 SHM 读取端

```bash
python -m FT300S.shm_read_test_client --shm-name ft300_sensor_frame --duration 5
```

## 运行 100Hz 基准测试

```bash
python -m FT300S.benchmark_100hz --duration 20 --target-hz 100
```

如果你想对比寄存器模式，可加 --modbus-mode。

基准测试输出：

- FT300S/runtime_benchmark/ft300_bench_*.csv：逐帧原始数据
- FT300S/runtime_benchmark/ft300_bench_*.summary.json：统计汇总

summary 关键字段：

- achieved_hz：实际平均采样频率
- miss_count：调度错过周期累计次数
- overrun_count：单次读取耗时超过目标周期的次数
- interval_*：相邻两帧启动间隔统计
- read_*：单次 read_frame 耗时统计
- lag_*：循环相对计划时刻的滞后统计

## 说明

- stream 模式需要安装 libscrc：pip install libscrc。
- 服务输出文件默认写入仓库根目录下的 runtime_frames，可通过 `--save-dir` 覆盖。
- 第一帧为 warmup 帧（frame_id=-1），仅用于共享内存 schema 探测。
