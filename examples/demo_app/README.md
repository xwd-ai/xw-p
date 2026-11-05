# 内容社区演示应用

一个最小的内容社区业务侧应用，展示如何用 `sdk/python` 接入 xw-notify：
创作者发帖，其他人点赞、评论、@提及，通知全部交给平台分发。

业务侧只做三件事：建档案、存帖子、在互动发生时触发一个事件。渠道选择、
模板渲染、点赞聚合、幂等去重都由平台负责，代码里看不到这些细节。

## 运行

先按仓库根目录 `README.md` 启动 xw-notify（API + 引擎），然后：

```bash
# 仓库根目录，SDK 不必安装，示例直接引用 sdk/python
api/.venv/bin/uvicorn examples.demo_app.app:app --port 8100
```

指向别的部署用环境变量（与 SDK 示例同一组）：

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `XW_BASE_URL` | `http://127.0.0.1:8000` | xw-notify API 地址 |
| `XW_API_KEY` | （空） | 部署配了 `XW_API_KEYS` 时必填 |
| `XW_ENVIRONMENT` | `development` | 环境标识 |

## 接口走一遍

```bash
BASE=http://127.0.0.1:8100

# 1. 登记创作者（email / device_token 是通知收件地址，随每次触发带上）
curl -s -X POST $BASE/creators -H 'Content-Type: application/json' \
  -d '{"id":"creator-1001","name":"小林","email":"lin@example.com","device_token":"fcm-9f2c1d"}'

# 2. 发帖
curl -s -X POST $BASE/posts -H 'Content-Type: application/json' \
  -d '{"id":"P-2026-1001","creator_id":"creator-1001","title":"我的第一支 Vlog"}'

# 3. 三次点赞：post_liked 按帖子聚合，收件箱里是一张累计卡片
for actor in 阿澈 小夏 沐沐; do
  curl -s -X POST $BASE/posts/P-2026-1001/like -H 'Content-Type: application/json' \
    -d "{\"actor\":\"$actor\"}"
done

# 4. 评论：comment_id 作幂等键，重发同一条不会通知两遍
curl -s -X POST $BASE/posts/P-2026-1001/comment -H 'Content-Type: application/json' \
  -d '{"actor":"阿澈","text":"拍得真好，求教程！","comment_id":"C-8842"}'

# 5. @提及：通知被 @ 的人（需先有档案）
curl -s -X POST $BASE/creators -H 'Content-Type: application/json' \
  -d '{"id":"creator-2002","name":"阿澈","device_token":"fcm-2222"}'
curl -s -X POST $BASE/posts/P-2026-1001/mention -H 'Content-Type: application/json' \
  -d '{"actor":"小林","target":"creator-2002","text":"@阿澈 你也来看看"}'

# 6. 读收件箱（站内信）
curl -s "$BASE/creators/creator-1001/inbox?unread=true"
```

互动接口返回触发回执（`triggerId` 等三标识）；通知本身是异步投递的，
收件箱要稍等片刻再查。帖子与档案存在进程内存，重启即空——演示项目要
展示的是接入方式，不是存储。

## 测试

假传输层记录请求、不联网，断言的是「业务侧把什么交给平台」：

```bash
api/.venv/bin/python -m pytest examples/demo_app -q
```
