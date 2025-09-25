# 每日打卡后端

FastAPI 实现的个人每日打卡服务，支持文字 + 图片上传，内置 JWT 鉴权和 SQLite 存储，默认静态文件保存在本地 media/ 目录。

## 目录结构

- pp/：FastAPI 应用源码（模型、路由、鉴权等）
- 	ests/：pytest 自动化测试
- equirements.txt：运行依赖
- equirements-dev.txt：运行 + 测试依赖
- .venv/：推荐的虚拟环境位置（本地创建）

## 环境准备

1. 安装 Python 3.10+（本地环境已是 3.13.7）
2. 在项目根目录创建虚拟环境：
   `powershell
   cd F:\AI
   python -m venv .venv
   .\.venv\Scripts\Activate.ps1
   `
3. 安装依赖：
   `powershell
   pip install -r requirements.txt
   # 如需跑测试：pip install -r requirements-dev.txt
   `

## 启动服务

1. 激活虚拟环境：.\.venv\Scripts\Activate.ps1
2. 启动开发服务器：
   `powershell
   uvicorn app.main:app --reload
   `
3. 默认监听 http://127.0.0.1:8000
4. Swagger 文档：http://127.0.0.1:8000/docs

## 环境变量（可选）

- DATABASE_URL：默认为 sqlite:///./checkin.db
- MEDIA_ROOT：图片保存目录，默认 media/
- SECRET_KEY：JWT 密钥，默认 change-me（部署时请修改）
- ACCESS_TOKEN_EXPIRE_MINUTES：访问令牌有效期（分钟，默认 60）
- ALLOWED_ORIGINS：CORS 白名单，多个域名用逗号分隔，默认 *

## 核心接口

| 方法 | 路径 | 说明 |
| ---- | ---- | ---- |
| POST | /auth/register | 注册用户，JSON：{"username": "alice", "password": "secret"} |
| POST | /auth/token | 登录换取 JWT，需使用 pplication/x-www-form-urlencoded 表单提交 username、password（示例见下） |
| GET | /users/me | 查看当前登录用户信息 |
| POST | /checkins | 新增打卡记录（multipart/form-data，字段见下文） |
| GET | /checkins | 分页查看自己的打卡列表（参数：skip、limit） |
| GET | /checkins/{id} | 查看单条打卡详情 |
| DELETE | /checkins/{id} | 删除打卡（同时删除已保存图片） |
| GET | /media/{path} | 访问已上传图片 |

### 登录接口调用示例

`ash
curl -X POST http://127.0.0.1:8000/auth/token \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "username=your_name&password=your_password"
`

在 Swagger UI 中，请先点击右上角 Authorize 按钮，按照表单模式填写 username、password，无需填写 grant_type。

### 打卡上传字段说明

- content：文字内容（必填）
- category：分类，如 itness、study（默认 general）
- checkin_date：日期字符串（YYYY-MM-DD，默认当天）
- iles：可选的图片文件，支持多个 iles 字段

#### 打卡上传示例

`ash
curl -X POST http://127.0.0.1:8000/checkins \
  -H "Authorization: Bearer <your_token>" \
  -F "content=规划了一小时英语学习" \
  -F "category=study" \
  -F "checkin_date=2025-09-22" \
  -F "files=@path/to/photo1.jpg" \
  -F "files=@path/to/photo2.jpg"
`

> 调用 /checkins 前需要在 Authorization: Bearer <token> 中携带登录获取的 JWT。

## 数据与文件存储

- 数据库：默认使用 SQLite 文件 checkin.db
- 图片：存储在 MEDIA_ROOT/user_<用户ID>/<日期>/ 目录下
- 删除打卡时会尝试删除对应图片文件

## 运行自动化测试

`powershell
.\.venv\Scripts\Activate.ps1
python -m pytest
`

测试会自动：注册、登录、上传打卡、校验查询结果、删除记录，并验证图片文件是否创建与删除。

## 部署提示

- 将 SECRET_KEY 改为强随机值
- 使用 Nginx 反向代理并开启 HTTPS
- 配置备份脚本定期备份数据库与 media/
- 随时可以把 DATABASE_URL 切换到 PostgreSQL、MySQL 等更稳健的数据库
