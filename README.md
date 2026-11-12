# storemetrics

门店设备测量 exposition 文本与 HTTP 抓取原文工具。

## 本地运行

使用 Python 3.11 标准库，无 pip 依赖。所有来源与业务数据都是本地合成数据，不连接真实用户系统。

```sh
python -m scripts.materialize
python -m storemetrics inspect fixtures/gauge.prom
python -m compileall -q .
python -m unittest discover -s tests -v
python -m scripts.demo
```

检查命令输出单份来源的结构摘要。`capture URL --method GET --state var` 仅接受 loopback HTTP，保存原始响应为 SHA256 文件，并在 SQLite captures 表登记；可用 `--header 'Name:Value'` 传头、`--body FILE` 传正文，不跟随重定向。检查器不执行多页、业务去重或正式接收。

## 抓取接收批

```sh
python -m scripts.source_server --port 8765           # 终端 1
python -m storemetrics scrape --config fixtures/scrape_config.json \
    --state var/scrape --all                           # 终端 2
python -m storemetrics query --state var/scrape --batches
python -m storemetrics query --state var/scrape --series --present yes
python -m storemetrics query --state var/scrape --samples --batch A#1
```

`fixtures/scrape_config.json` 是本地示例，默认指向 `127.0.0.1:8765`，按 `--port` 调整；
`scrape` 不使用 `--system-clock` 时使用确定性的逐步注入时钟，`--clock-start` 可指定起点，
`--release-stale SECONDS` 按注入时钟释放超时的未完成抓取。每次抓取是一个接收批
`<endpoint>#<attempt>`，失败批也留诊断与载荷摘要，但只有完整成功批推进系列存在状态。
配置中的多个端点各自独立推进；单个端点最多一个未完成抓取；重启只读取 SQLite 中的
持久化完成时间，不凭进程内定时器推断缺失抓取。协议细节见 docs/source-contract.md，
核验清单见 docs/acceptance.md。范围不含告警、预测或看板。

## 可复现容器

```sh
docker build -t storemetrics:local .
docker run --rm --network none storemetrics:local python -m unittest discover -s tests -v
docker run --rm --network none storemetrics:local
```

镜像固定基础摘要。构建在正式运行前完成；运行和测试不安装包、不下载数据。`scripts/demo.py` 展示真实业务闭环：一次成功、一次解析失败、再一次成功删除系列、超预算整批拒绝、非有限值与转义标签留存；输出为 JSON 行，实测远小于 90 秒。

## 协议与来源

- docs/source-contract.md 给出支持子集、字段及失败语义。
- docs/acceptance.md 给出业务验收边界。
- fixtures/source.json 是本地可重放 HTTP 路由；`python -m scripts.source_server --port 8765` 启动，`/__requests` 可检查实际请求。
- scripts/materialize.py 将版本化 base64 二进制夹具还原为原始字节，重复执行安全；二进制文件不是用户附件。
- journal.py 只记录原始响应，不代替新增流程自己的事务和恢复设计。
