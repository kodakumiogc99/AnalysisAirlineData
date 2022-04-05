---
tags: Big Data
---

# Big Data HW2

309555024 郭家宏


## Dataset

[Airline on-time performance](https://community.amstat.org/jointscsg-section/dataexpo/dataexpo2009)
[USA shape file](https://www.efrainmaps.es/english-version/free-downloads/united-states/)


## Platform

- PySpark via Jupyter Notebook
- Hardware info
    - CPU: 32 Core
    - Mem: 504G
    - Memory block size: 2G

## Questions

### Q1: Find the maximal delays (you should consider both ArrDelay and DepDelay) for each month of 2007.

- 第一個問題我用以下幾個步驟
    - 過濾出 2007 年的資料。
    - 以月份來分群。
    - 求最大值和最小值。

```python=
q1 = df.filter(df.Year == 2007).select(df.Month, df.ArrDelay, df.DepDelay)
MaxArr = q1.groupby('Month').max('ArrDelay')
MaxDep = q1.groupby('Month').max('DepDelay')
MinArr = q1.groupby('Month').min('ArrDelay')
MinDep = q1.groupby('Month').min('DepDelay')
```

- 以下是我的結果

![](https://i.imgur.com/03Wv2NF.png)
![](https://i.imgur.com/HtUqxJf.png)

#### 下面我額外分析了每月 delay 的最長時間

- DepDelay 和 ArrDelay 看起來時間差不多，應該是 DepDelay 常常導致 ArrDelay。
- 我用 max DepDelay 當索引，去看同一班的 ArrDelay，發現也會是 max ArrDelay, 故驗證我的猜想。

```python=
Y07 = df.filter(df.Year==2007)
Y07.join(q1, Y07.ArrDelay==q1['max(ArrDelay)']).select(Y07.ArrDelay,
        Y07.DepDelay, q1['max(ArrDelay)'], q1['max(DepDelay)'])
Y07.join(q1, Y07.ArrDelay==q1['min(ArrDelay)']).select(Y07.ArrDelay,
        Y07.DepDelay, q1['min(ArrDelay)'], q1['min(DepDelay)'])
```

![](https://i.imgur.com/fftkSR0.png)

- 統計一下 2007 年的每月延遲次數，以七月最多。

![](https://i.imgur.com/RNJ3hxe.png)
![](https://i.imgur.com/5hYoHTS.png)

- 比較每一年的延遲數 (2000~2007)
    - 最多的是 2007.
    - 最少的是 2002.
- 可以看出每年最常出現延遲的是 6~8 月和 12 月。可能是受天氣影響。

![](https://i.imgur.com/OIm3bIn.png)
![](https://i.imgur.com/DNr1fGg.png)

- 把造成延遲的原因也畫出來，發現 carrier, NAS 和 late air craft 是主因。
- 也可發現把這些原因的數量加起來還跟延遲總數有一段差距。可能有些原因沒記錄。

![](https://i.imgur.com/dfOaVEj.png)

- 比較一下每年延遲的佔比，發現 12 月延遲的比例最高。

![](https://i.imgur.com/JigrhHV.png)

---

### Q2: How many flights were delayed caused by security between 2000~2005? Please show the counting for each year.

- 直接計算 2000~2005 SecurityDelay 大於 0 的次數。

```python=
q2 = df.filter((df.Year >= 2000) & (df.Year <= 2005))
q2 = q2.filter(q2.SecurityDelay > 0)
q2.groupby('Year').count().show()
display(q2.count())
```

- 結果:

![](https://i.imgur.com/ycMVS0z.png)

- 2000~2005 SecurityDelay 的總次數: 18525

- 計算 2000~2008 所有次數

![](https://i.imgur.com/Syi6qrf.png)
![](https://i.imgur.com/jhRjwY2.png)
![](https://i.imgur.com/ZXmpSds.png)

- 可以看出 2003 年 5 月以前沒有記錄。
- 每年大概都是 7 月和 12 月佔比最高。

---

### Q3: List Top 5 airports which occur delay most and least in 2008. (Please show the IATA airport code)

- 我將 ArrDelay 和 DepDelay 分開算。
- 如 DepDelay 發生延遲，則 Origin 的機場算一次，ArrDelay 發生延遲，則 Dest 的機場算一次。
- 最後將兩個加起來。

```python=
q3A = df.filter((df.ArrDelay >0) & 
                (df.Year == 2008) ).groupby('Dest').count()
q3D = df.filter((df.DepDelay > 0) & 
                (df.Year == 2008)).groupby('Origin').count()
q3 = q3D.join(q3A, q3A.Dest == q3D.Origin)
q3 = q3.withColumn('Total', q3.DestCount+q3.OriginCount)
q3.sort('Total', ascending=False).show(5)
q3.sort('Total', ascending=True).show(5)

```

- Top 5 Most

![](https://i.imgur.com/AyHfCPr.png)

- Top 5 Least

![](https://i.imgur.com/Z3O0DYk.png)

---

### Other Analysis

- 把機場畫到美國的地圖上，地圖以地區作區分，分成九大區

![](https://i.imgur.com/xtnvKSZ.png)
![](https://i.imgur.com/CKPrOix.png)

- 計算每區的機場數量
    - 機場最多的區域是 South Atlantic.
    - 機場最少的區域是 New England.
![](https://i.imgur.com/1q6M134.png)
![](https://i.imgur.com/asqY2Yf.png)

- 計算每區機場起飛的次數
    - 起飛次數最多的區域是 South Atlantic.
    - 起飛次數最少的區域是 New England.
![](https://i.imgur.com/oSIkcnq.png)
![](https://i.imgur.com/Wzaz1rQ.png)

- 計算每區機場降落的次數
    - 降落次數最多的區域是 South Atlantic.
    - 降落次最少的區域是 New England.
![](https://i.imgur.com/UdmdLOr.png)
![](https://i.imgur.com/WysRvWx.png)

- 看起來飛機的航班總數和機場數量成正比.

---

- 將 Origin 和 Dest 看成一對當成一條航線，可以算出有 7289 條航線。

![](https://i.imgur.com/UgFGcza.png)

- 可以將航線的兩端連起來，得到一張這樣的圖，但看起來很複雜。

![](https://i.imgur.com/3OXoWvy.jpg)

- 將數量前十的航班列出來，發現都是短程且多集中在西部。

![](https://i.imgur.com/wMIGpk7.png)
![](https://i.imgur.com/crutyqb.png)

- 將數量前二十的航班列出來，增加了一些中程航班，大多在東部和中部。

![](https://i.imgur.com/j5rimje.png)
![](https://i.imgur.com/YHNSe1h.jpg)

- 將數量前一百的航班列出來，大多集中在南部，北部幾乎沒有。

![](https://i.imgur.com/pijtRC2.jpg)

- 以上是我的分析～