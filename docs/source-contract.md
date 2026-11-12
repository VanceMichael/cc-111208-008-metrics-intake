# 门店设备文本协议

来源为本地 OpenMetrics 1.0 子集，UTF-8，支持 gauge、counter、classic histogram，HELP/TYPE/UNIT 和必需的末尾 # EOF；不含 native histogram。标签双引号包裹，支持换行、反斜杠和双引号转义，重复标签拒绝，排序后的标签名值与端点、指标名共同决定系列身份。值保留有限精确数及 NaN、+Inf、-Inf，不将非有限值存成数据库 NULL。可选时间戳来自设备，另存注入的本地采集时间；exemplar 的标签、值、时间不并入样本标签。

直方图 le 按数值排序，桶非负且累计，必须有 +Inf 桶并等于 count，sum 单独保留。同批相同系列的值与时间戳都一致才允许重复。端点系列预算计入直方图各分量，写入前整体检查。每个端点最多一份未完成抓取，失败端点不阻塞其他端点；只有完整有效的成功抓取能推进系列存在状态。

/device/A 依次返回完整测量、缺 EOF、成功空集合，B 独立。缺失 EOF、语法错误、超预算或超时不能生成系列消失。当前检查器只展示无标签标量，新增 scrape 应支持上述协议且留下可查询接收批。范围不含告警、预测或看板。

## scrape 接收批实现约定

- 配置为 JSON：`endpoints`（`name` + loopback `url`）、`series_budget`、`interval_seconds`、`timeout_seconds`。
- 每次尝试 = 一个接收批 `batch_id = "<endpoint>#<attempt>"`，无论成功或失败都写入 `scrape_batches`，含原始载荷 SHA256 与诊断；失败批次 `series_written = 0`。
- 样本、系列身份（`series` 表，`present` 标记）与抓取结果均可经 `storemetrics query` 查询。
- 设备时间戳以文本原样保留并另存整数纳秒列；本地采集时间为注入时钟值，不据源时钟回拨推算。
- 系列身份 = 端点 + 指标名 + 排序后的标签名值；`present` 只由完整成功批次推进，删除以 `series_events.disappeared` 留痕。
- exemplar 的标签、值、时间存于样本的 `exemplar` JSON 列，不并入样本标签。

