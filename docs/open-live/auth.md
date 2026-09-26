# 鉴权

> 整理自哔哩哔哩直播开放平台《开放平台-直播&互玩接入文档》。原文：[鉴权](https://open-live.bilibili.com/document/74eec767-e594-7ddd-6aba-257e8317c05d)。
>
> 统一鉴权与公共错误码。直播桥对开放平台的全部 HTTP 请求都走这套签名。

## 环境域名

**注意：所有API都要用以下域名请求。**

- 正式环境：[https://live-open.biliapi.com](https://live-open.biliapi.com)

**注意：接口method均为POST，请不要在URL后拼接GET参数，content-type的类型必须是JSON。**

## 公共参数

下表描述的公共请求头（Request Header）适用于通过URL发送HTTP POST请求调用API。

| 名称 | 类型 | 是否必填 | 描述 |
| --- | --- | --- | --- |
| Accept | String | 是 | 接受的返回结果的类型。目前只支持JSON类型，取值：application/json。 |
| Content-Type | String | 是 | 当前请求体（Request Body）的数据类型。目前只支持JSON类型，取值：application/json。 |
| x-bili-content-md5 | String | 是 | 请求体的编码值，根据请求体计算所得。算法说明：将请求体内容当作字符串进行MD5编码。 |
| x-bili-timestamp | String | 是 | unix时间戳，单位是秒。请求时间戳不能超过当前时间10分钟，否则请求会被丢弃。 |
| x-bili-signature-method | String | 是 | 签名方式。取值：HMAC-SHA256 |
| x-bili-signature-nonce | String | 是 | 全网唯一随机字符串。用于防止网络重放攻击，建议直接使用UUID |
| x-bili-accesskeyid | String | 是 | AccessKeyId |
| x-bili-signature-version | String | 是 | 1.0 |
| Authorization | String | 是 | 请求签名（注意生成的签名是小写的）。关于请求签名的计算方法，请参见签名机制 |

## 签名机制

**注意：请求所有API都要有签名。**

### 构建完整的待签名字符串

抽取带”x-bili-“前缀的自定义header，按字典排序拼接，构建完整的待签名字符串：

```go
"x-bili-accesskeyid:$accesskeyidValue" + "\n" +
"x-bili-content-md5:$contentMd5Value" + "\n" +
"x-bili-signature-method:HMAC-SHA256" + "\n" +
"x-bili-signature-nonce:$signatureNonceValue" + "\n" +
"x-bili-signature-version:1.0" + "\n" +
"x-bili-timestamp:$timestamp"
```

待签名字符串示例：

```go
x-bili-accesskeyid:xxxx
x-bili-content-md5:fa6837e35b2f591865b288dfd859ce9d
x-bili-signature-method:HMAC-SHA256
x-bili-signature-nonce:ad184c09-095f-91c3-0849-230dd3744045
x-bili-signature-version:1.0
x-bili-timestamp:1624594467
```

### 生成签名

对得到的字符串，使用AccessKey Secret进行HMAC-SHA256算法进行签名。`python`示例代码如下：

```python
import hmac
from hashlib import sha256
def generate_signature():
    demo_access_secret_key = "JzOzZfSHeYYnAMZ"
    # 注意这里的第一行开头没有换行符
    demo_header_str = """x-bili-accesskeyid:xxxx
x-bili-content-md5:fa6837e35b2f591865b288dfd859ce9d
x-bili-signature-method:HMAC-SHA256
x-bili-signature-nonce:ad184c09-095f-91c3-0849-230dd3744045
x-bili-signature-version:1.0
x-bili-timestamp:1624594467""" # 这里的最后一行结尾也没有换行符。真实生成的head_str中最后一行也是没有换行符
    # 把access_secret_key和header_str转为bytes, 再用SHA-256算法生成签名
    signature = hmac.new(demo_access_secret_key.encode(), demo_header_str.encode(), digestmod=sha256).hexdigest()
    # a81c50234b6bbf15bc56e387ee4f19c6f871af2f70b837dc56db16517d4a341f
    print(signature)
if __name__=='__main__':
    generate_signature()
```

返回结果：

> 返回的http status code均为200，第三方需要根据返回内容的code判断是否正常。

| 名称 | 类型 | 是否必填 | 例子 | 描述 |
| --- | --- | --- | --- | --- |
| code | int | 是 | 0 | 返回码，公共错误码参考下方 |
| message | string | 是 | 成功 | 返回信息，公共错误信息参考下方 |
| request_id | string | 是 | 1409787829088702464 | 请求id |
| data | struct | 是 |  | 业务信息，具体请参考各业务接口返回值 |

## 参考DEMO（Python3 & GO）

> [demo-python.zip](https://activity.hdslb.com/blackboard/static/20240311/cc51945399520f91bb65629803b43743/240311-py-demo-new.zip)
> [demo-go.zip](https://activity.hdslb.com/blackboard/static/20240311/cc51945399520f91bb65629803b43743/240311-go-demo-new.zip)

## 公共错误码

| 错误代码 | 描述 | 备注 |
| --- | --- | --- |
| 4000 | 参数错误 | 请检查必填参数，参数大小限制 |
| 4001 | 应用无效 | 请检查header的x-bili-accesskeyid是否为空，或者有效 |
| 4002 | 签名异常 | 请检查header的Authorization |
| 4003 | 请求过期 | 请检查header的x-bili-timestamp |
| 4004 | 重复请求 | 请检查header的x-bili-nonce |
| 4005 | 签名method异常 | 请检查header的x-bili-signature-method |
| 4006 | 版本异常 | 请检查header的x-bili-version |
| 4007 | IP白名单限制 | 请确认请求服务器是否在报备的白名单内 |
| 4008 | 权限异常 | 请确认接口权限 |
| 4009 | 接口访问限制 | 请确认接口权限及请求频率 |
| 4010 | 接口不存在 | 请确认请求接口url |
| 4011 | Content-Type不为application/json | 请检查header的Content-Type |
| 4012 | MD5校验失败 | 请检查header的x-bili-content-md5 |
| 4013 | Accept不为application/json | 请检查header的Accept |
| 5000 | 服务异常 | 请联系B站对接同学 |
| 5001 | 请求超时 | 请求超时 |
| 5002 | 内部错误 | 请联系B站对接同学 |
| 5003 | 配置错误 | 请联系B站对接同学 |
| 5004 | 房间白名单限制 | 未上架的应用，只能连接开发者自己的直播间，如果已上架还提示，请联系B站对接同学 |
| 5005 | 房间黑名单限制 | 请联系B站对接同学 |
| 5011 | 应用权限限制 | 应用存在黑白名单限制，默认未上架应用只允许开发者自己连接，如已上架且提醒该问题，请联系B站对接同学 |
| 6000 | 验证码错误 | 验证码校验失败 |
| 6001 | 手机号码错误 | 检查手机号码 |
| 6002 | 验证码已过期 | 验证码超过规定有效期 |
| 6003 | 验证码频率限制 | 检查获取验证码的频率 |
| 6010 | 房间号不能为空 | 房间号不能为空 |
| 6011 | 没有查询到房间 | 没有查询到房间 |
| 6012 | 主播信息为空 | 主播信息为空 |
| 6013 | 互玩游戏关闭失败 | 互玩游戏关闭失败 |
| 6014 | 插件关闭失败 | 插件关闭失败 |
| 6015 | 直播工具关闭失败 | 直播工具关闭失败 |
| 7000 | 不在游戏内 | 当前房间未进行互动游戏 |
| 7001 | 请求冷却期 | 上个游戏正在结算中，建议10秒后进行重试 |
| 7002 | 房间重复游戏 | 当前房间正在进行游戏,无法开启下一局互动游戏 |
| 7003 | 心跳过期 | 当前game_id错误或互动游戏已关闭 |
| 7004 | 批量心跳超过最大值 | 批量心跳单次最大值为200 |
| 7005 | 批量心跳ID重复 | 批量心跳game_id存在重复,请检查参数 |
| 7007 | 身份码错误 | 请检查身份码是否正确 |
| 7008 | 插件重复开启 | 插件重复开启 |
| 7009 | 无道具投放权限 | 无道具投放权限 |
| 7010 | 超过上限，同一个应用单个直播间最多同时打开5个 | 单个直播间对于同一个插件\工具最多能启动5个连接，请检查start和心跳维持数量 |
| 8002 | 项目无权限访问 | 确认项目ID是否正确 |
