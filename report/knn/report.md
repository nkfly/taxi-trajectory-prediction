# KNN based model
# Idea
The idea comes from the fact that any routes of a destination passes through similar paths near the destination. Say in other words, anyone going to the entrance of NTU must pass through either Roosevelt‎ Rd. or Xinsheng S. Rd. Hence, by computing the _similarity_ of two taxi routes, we can pick _top N_ training taxi routes which are the most similar to the testing route, and find the most promising destination among the training routes.
In details, we should define the _similarity_ between two routes and find a way to tell the _most promising destination_ among the top N similar routes.

# Implementation
## define the similarity
For any two polylines, ![route 1](eq1.svg) and ![route 2](eq2.svg) where ![ai](eq3.svg) and ![bi](eq4.svg) are coordinates consisting of longitude and latitude, and ![n <= m](eq11.svg). We define the distance as
![distance function](eq5.svg) subject to ![subject to](eq6.svg)
where ![wi](eq12.svg) are the weights

![distance](img1.svg)

Given the training set of polylines, ![training set](eq8.svg) and a test polyline ![test polyline](eq9.svg). We can pick top N similar polylines similar to ![test polyline](eq9.svg), which is ![similar polylines](eq10.svg).

We can pick the most promising destination by
* the destination of the most similar polyline
* majority voting
![KNN](img2.svg)
