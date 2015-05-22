### Kaggle link: https://www.kaggle.com/c/pkdd-15-predict-taxi-service-trajectory-i


# 5/16 progress update
###Model Li-Yuan
#####假設: 這麼多的training data route的destination已經包含了城市中大部份可能的destination，所以可以把那些destination map成一個label，就使得這個問題變為classification problem。
#####方法簡介: 依據test data的維度，去使用當初依據相對等維度所建的model，來做prediction。在feature selection上，就是使用last k points，然後把向量的因素考慮進去，也就是會把兩點的delta當作feature，而非點的絕對經緯度。
  - Quick Test:先用簡單的model，在training data set上做這個concept的驗證，用cross-validation的方式去看看map成classification的problem的話accuracy如何。
  - 未來展望:若是有效，則繼續從其提供的資料找出有用的feature，做進一步的feature selection，然後可以改採complexity更強大的model如dnn。
  
###Model Allen
#####假設: 計程車司機會走「到目的地的最短路徑」
#####方法簡介: 透過「方向」和「路程長度」找「目的地」集合C中最有可能的目的地ci
  - 方向: 給定目前已經走過的路徑S，最後到達目的地為ci的機率為p(ci|S)。若目的地在S延伸出去的方向上，則有較高的機率p到達目的地ci
  - 路程長度: 透過training data算出(到達目的地ci)的總路徑長度的機率分佈，到達ci的路程長度的機率分佈越高代表目的地越有可能是ci
  - 目的地: 透過training data找出可能的目的地集合C以及計程車到達某目的地ci的機率。例:平均每台計程車到達台北車站(ci)的機率是60%
  - 進度備忘: 緯度要看到多細(小數點後第幾位)。要用多少%的training data來train。我的電腦跑不動，想借工作站。兩個方法「方向」、「路程長度」的結果要如何結合(目前只是乘在一起)

###Model Alex
#####假設: 計程車司機從A點到鄰近B的transition probability不變
#####方法簡介: 透過「transition probablity」和「路程長度」找「目的地」集合C中最有可能的目的地ci
  - 方向: 給定目前已經走過的路徑S，設定長度threshold，最後機率最高的目的地為ci。
  - 路程長度: 將長度分為好幾個區間（e.g.1km, 1.5km, 2km,...)
  - 目的地: 比較每個長度的目的地在data裡面當作終點的機率，最高者輸出。
  
# taxi-trajectory-prediction
- http://www.vldb.org/pvldb/vol6/p1198-xue.pdf    by Alex
- http://ieeexplore.ieee.org/stamp/stamp.jsp?tp=&arnumber=6544830
- http://msr-waypoint.com/en-us/um/people/jckrumm/Publications%202006/efficient%20routes%20camera%20ready.pdf   by Allen Lee
- https://www.ri.cmu.edu/pub_files/2006/0/driver_intent.pdf
- http://www.icmu.org/icmu2012/papers/FP-5.pdf   by @nkfly
- http://www.ruizhang.info/publications/Vldbj2014-DestinationPrediction.pdf

## In this challenge, we are going to build a predictive framework that is able to infer the final destination of taxi rides in Porto, Portugal based on their (initial) partial trajectories. The output of such a framework must be the final trip's destination.

## basic but reasonable solution
1. survey paper for possible solutions
2. formulated question, especially its states
3. apply at least 1 solution to solving the formulated question
4. upload the reasonable solution to Kaggle

## Improve the solution with various models
1. model trajectory choosing as stochastic events
2. search-based algorithm
3. model combination

## Visualization of our algorithm and search precess for better understanding and demo effect
1. web ui to show the prediction process
2. figure to show the performance improvement and comparison of models

