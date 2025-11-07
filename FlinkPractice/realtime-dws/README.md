# DWS 层

DWS 层表名命名规范 dws_数据域_统计粒度_业务过程_统计周期（window）

## realtime-dws-traffic-source-keyword-page-view-window

流量域搜索关键词粒度页面浏览各窗口汇总表

读取页面的搜索关键字，进行粉刺处理，并统计每个周期内每个词的数量

## realtime-dws-traffic-vc-ch-ar-is_new-page-view-window

流量域版本-渠道-地区-访客类别粒度页面浏览各窗口汇总表

汇总会话数、页面浏览数、浏览总时长、独立访客数四个度量字段。并将维度和度量数据写入 Doris 汇总表



