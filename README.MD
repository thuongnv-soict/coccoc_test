# CocCoc Test

### 1. Description
CocCoc test

### 2. Requirements
```
openjdk 11.0.9 2020-10-20
OpenJDK Runtime Environment (build 11.0.9+11-Ubuntu-0ubuntu1.18.04.1)
OpenJDK 64-Bit Server VM (build 11.0.9+11-Ubuntu-0ubuntu1.18.04.1, mixed mode, sharing)
```


### 3.1 Data Processing
_View in Answers.pdf_

#### 3.2 Algorithms

**Idea:**

+ Split the large file into many smaller files.
+ Sort  each file by **objectId**.
+ Combine all sorted files into one file. 
+ Create a temporary list contains all first object of each file then sort item in it.
  + Get and remove the first object in above list. Write it to output file. 
  + Insert the next object from the file of removed object to the above list.  If there is no line in this file, ignore it.
  + Use Binary Search for finding the position that new object will be located.
  + Loop until there is no object in temporary list. 

#### 3.3  Categories of objects are updated in real-time
_View in Answers.pdf_

### 4. Documentation
Answers.pdf

or

https://drive.google.com/file/d/12ltnAxyHB3gAVYu8j3zV2dlYavwYSG4N/view?usp=sharing
