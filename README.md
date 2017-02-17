##链家爬虫

https://github.com/lanbing510/LianJiaSpider 已经不能用了
这是它的深圳版加改良版

###数据库格式(方便作为机器学习的数据)
CREATE TABLE xiaoqu (name TEXT primary key UNIQUE, district TEXT, economic_circle TEXT, style TEXT, year INTEGER, metro_info TEXT);

CREATE TABLE chengjiao (href TEXT primary key UNIQUE, name TEXT, room INTEGER, hall INTEGER, size INTEGER, direction TEXT, other_info TEXT, lift_info TEXT, deal_date date, total_price INTEGER, unit_price INTEGER, floors INTEGER, build_time INTEGER);


###数据格式
松坪山生活区公寓|南山区|科技园|http://sz.lianjia.com/chengjiao/c2411450430271150/|0|
光大村|南山区|科技园|http://sz.lianjia.com/chengjiao/c2411049602738/|0|近地铁11号线南山站
山语海|南山区|科技园|http://sz.lianjia.com/chengjiao/c246946024600333/|0|
宏观苑|南山区|科技园|http://sz.lianjia.com/chengjiao/c2411049620736/|0|近地铁11号线南山站
中熙君南山|南山区|科技园|http://sz.lianjia.com/chengjiao/c2411062963264/|0|近地铁2号线(蛇口线)登良站


http://sz.lianjia.com/chengjiao/105100103866.html|一辉花园|4|2|162|南 | 其他 | 有电梯|2016-05-09 00:00:00|1070|65960|7|2002
http://sz.lianjia.com/chengjiao/SZNS90972491.html|一辉花园|5|2|310|南 北 | 其他 | 有电梯|2015-12-25 00:00:00|1730|55791|8|1997
http://sz.lianjia.com/chengjiao/SZNS90926381.html|万丰园|2|2|70|南 | 精装||2016-01-09 00:00:00|353|50422|7|0
http://sz.lianjia.com/chengjiao/105100467450.html|128大院|5|2|168|南 北 | 其他 | 无电梯|2016-12-07 00:00:00|670|39881|6|1985
http://sz.lianjia.com/chengjiao/105100168029.html|128大院|4|2|145|南 北 | 简装 | 无电梯|2016-11-13 00:00:00|570|39189|7|1986