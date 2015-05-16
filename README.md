# 5/16 progress update
1. Model Allen
  A. 假設: 計程車司機會走「到目的地的最短路徑」
  B. 方法簡介: 透過「方向」和「路程長度」找「目的地」集合C中最有可能的目的地ci
    - 方向: 給定目前已經走過的路徑S，最後到達目的地為ci的機率為p(ci|S)。若目的地在S延伸出去的方向上，則有較高的機率p到達目的地ci
    - 路程長度: 透過training data算出(到達目的地ci)的總路徑長度的機率分佈，到達ci的路程長度的機率分佈越高代表目的地越有可能是ci
    - 目的地: 透過training data找出可能的目的地集合C以及計程車到達某目的地ci的機率。例:平均每台計程車到達台北車站(ci)的機率是60%
  
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

