# CocCoc Test

### 1 Description

### 2 Requirements
```
openjdk 11.0.9 2020-10-20
OpenJDK Runtime Environment (build 11.0.9+11-Ubuntu-0ubuntu1.18.04.1)
OpenJDK 64-Bit Server VM (build 11.0.9+11-Ubuntu-0ubuntu1.18.04.1, mixed mode, sharing)
```

### 3 Answers
#### 3.1 Data Processing
Idea:

+ For each category, save it into a HashMap with key is **category_id**, value is **category_count**.

+ If **category_id** is not exists in HashMap, add new. If exists, add **category_count** to existed value. 

Code: _src/vn.coccoc/DataProcessing_

#### 3.3 Algorithms
Idea:

+ Split large file into many smaller files.

+ Sort  each file by object_id.

+ Combine all sorted files into one file. 

+ Create a list has size equal number of sorted files. 

Code: _src/vn.coccoc/Algorithm_

#### 3.5 Algorithms

##### 4. Documentation
https://interspacevn.atlassian.net/l/c/1fYYTrUu
