/*
This is a distributed actor system.

TODO(soheil): Add a complete description.

On each hive, we run the same collection of actor, but each control specific
shards throughout the cluster.

[Inst. A1] [Inst. A2]    [Inst. B1]
    |      /                 |
   [Actor A]             [Actor B]
 ----------------------------------
|             Hive 1              |
 ----------------------------------
                |
 ----------------------------------
|             Hive 2              |
 ----------------------------------
   [Actor A]             [Actor B]
       |                 /      |
   [Inst. A3]    [Inst. B3]  [Inst. B4]
*/

package bh
