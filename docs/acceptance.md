# 接收流程核验

`storemetrics scrape` 已实现一次抓取 = 一个独立可核对接收批。以下条目由
`tests/test_scrape.py` 的端到端回归覆盖；原有 `tests/test_baseline.py` 只证明原功能。

## 已核验行为

- **标签与身份**：标签顺序不决定系列身份；转义引号、换行、反斜杠正确还原；重复标签整批拒绝；NaN / +Inf / -Inf 保留 `value_kind` 与原始文本，不写成 NULL。
- **重复样本**：同批同系列只有值与时间戳（及 exemplar）完全一致才合并计数，冲突行整批拒绝，不采用“最后一行获胜”。
- **直方图**：`le` 按数值排序，桶非负且累计，必须有 `+Inf` 桶且等于 `_count`；按标签集分别校验。
- **失败不制造消失**：缺 EOF、语法错误、坏直方图、超时、超预算的批次只记录失败诊断与原始载荷摘要，不推进系列存在状态。
- **超预算**：超过 `series_budget`（计入直方图各分量）时写入前整体拒绝，SQLite 中不出现该批任何样本或系列行。
- **端点独立**：A 失败不阻塞 B；两个端点各自推进尝试号与存在状态。
- **时间**：设备时间戳原样保留（含源时钟回拨），另存注入时钟给出的本地采集时间，不生成线性时间。
- **排他/重启**：每个端点最多一个未完成抓取（`scrape_claims` 以 IMMEDIATE 事务抢占）；任务到期判断只读 SQLite 中持久化的完成时间，重启不凭进程内定时器推断缺失；崩溃测试启动子进程并实际 kill。
- **超时**：`timeout_seconds` 注入 `transport.capture`，超时记为 `timeout` 失败。

## 复现

```sh
python -m unittest discover -s tests -v
python -m scripts.demo         # JSON 行输出，<90 秒
```

允许扩充 `fixtures/source.json` 与 `fixtures/scrape.json`。运行本地服务后可访问
`/__requests` 核对真实方法、请求头和正文。演示与测试均使用新的临时状态目录。
正式演示范围限于上述接收闭环，不含告警、预测或看板。
