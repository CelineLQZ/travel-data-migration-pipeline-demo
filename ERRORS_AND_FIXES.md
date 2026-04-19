# Common Errors & Fixes

## 1. Python / pip 版本不一致
**错误**: `ModuleNotFoundError: No module named 'faker'`  
**原因**: `python` 和 `pip` 指向不同版本（系统有多个 Python）  
**修复**: 用虚拟环境统一版本
```bash
python3.12 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

---

## 2. .env 文件没有保存
**错误**: S3 上传失败，bucket 名还是 `your-bucket-name`  
**原因**: `.env` 文件内容没有实际修改，还是示例值  
**修复**: 确认保存后用 `cat .env` 验证内容

---

## 3. EMR Serverless 权限缺失
**错误**: `AccessDeniedException: emr-serverless:GetApplication`  
**原因**: IAM 用户没有 EMR Serverless 权限  
**修复**: 给 **用户**（不是角色）添加内联策略
```json
{"Effect": "Allow", "Action": "emr-serverless:*", "Resource": "*"}
```

---

## 4. 把策略加到了 Role 而不是 User
**错误**: 内联策略显示 0 个关联实体，权限不生效  
**原因**: 误把策略加到了 `travel_industry_demo_role`，而不是 `travel-industry-demo-user`  
**修复**: 去 IAM → **用户** → 添加内联策略

---

## 5. EMR Application 在错误的 Region
**错误**: `ResourceNotFoundException: Application does not exist`  
**原因**: EMR application 在 `us-east-1`，但代码用的是 `eu-north-1`  
**修复**: 用 CLI 在正确 region 重建 application
```bash
aws emr-serverless create-application --name demo --type SPARK --release-label emr-7.11.0 --region eu-north-1
```

---

## 6. Airflow connection region 被 docker-compose 覆盖
**错误**: Airflow UI 改了 region，但仍然用旧 region  
**原因**: `docker-compose.yml` 里 `AIRFLOW_CONN_AWS_DEFAULT` 硬编码了 region  
**修复**: 修改 `docker-compose.yml` 里的 region 值，重启 Docker

---

## 7. iam:PassRole 权限缺失
**错误**: `AccessDeniedException: iam:PassRole`  
**原因**: IAM 用户没有权限把 role 传给 EMR  
**修复**: 添加内联策略
```json
{"Effect": "Allow", "Action": "iam:PassRole", "Resource": "arn:aws:iam::ACCOUNT_ID:role/ROLE_NAME"}
```

---

## 8. EMR vCPU 配额不足
**错误**: `account has reached the service limit on the maximum vCPU`  
**原因**: 新账户 EMR Serverless 默认配额很低  
**修复**: 降低 Spark 资源配置
```
--conf spark.executor.cores=1
--conf spark.executor.memory=2g
--conf spark.executor.instances=1
```

---

## 9. S3 跨 Region 超时
**错误**: `ApiCallTimeoutException` 访问 S3  
**原因**: EMR 在 `us-east-1`，S3 bucket 在 `eu-north-1`，跨 region 超时  
**修复**: 确保 EMR application 和 S3 bucket 在同一个 region

---

## 10. Spark job 读不到 S3_BUCKET 环境变量
**错误**: `KeyError: 'S3_BUCKET'`  
**原因**: EMR Serverless 不会自动继承 Airflow 的环境变量  
**修复**: 在 DAG 的 `sparkSubmitParameters` 里显式传入
```
--conf spark.emr-serverless.driverEnv.S3_BUCKET=bucket-name
--conf spark.executorEnv.S3_BUCKET=bucket-name
```

---

## 11. Snowflake Account Identifier 格式错误
**错误**: `404 Not Found` 连接 Snowflake  
**原因**: account identifier 格式不完整，缺少 region 和 cloud 后缀  
**修复**: 用完整格式 `bq45936.eu-west-3.aws`（从 `SYSTEM$ALLOWLIST()` 查询获取）

---

## 12. Snowflake COPY INTO 遇到空文件报错
**错误**: `Invalid: Parquet file size is 0 bytes`  
**原因**: Spark 输出目录包含 `_SUCCESS` 空文件，Snowflake 尝试解析时报错  
**修复**: 在 `COPY INTO` 语句加上 `ON_ERROR = SKIP_FILE`

---

## 13. Plotly subplot 类型不兼容
**错误**: `Trace type 'table' is not compatible with subplot type 'xy'`  
**原因**: `make_subplots` 没有声明第3行是 `table` 类型  
**修复**: 在 `make_subplots` 加上 `specs` 参数
```python
specs=[[{"type": "xy"}], [{"type": "xy"}], [{"type": "table"}]]
```
