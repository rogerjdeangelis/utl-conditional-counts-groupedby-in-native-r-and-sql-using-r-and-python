%let pgm=utl-conditional-counts-groupedby-in-native-r-and-sql-using-r-and-python;

Conditional counts groupedby in native r and sql usn r and python

github
https://tinyurl.com/bdnz4hkr
https://github.com/rogerjdeangelis/conditional-counts-groupedby-in-native-r-and-sql-usn-r-and-python

  SOLUTIONS
      1. wps sql

      2, native r
         https://stackoverflow.com/users/2094893/robert-hacken

      3. wps proc r sql (note outer join is not supported in sqllite use mySQL?   ----*/
         /*----  It looks like the left join will not return all left rows        ----*/
         /*----  when the right table has any subsetting                          ----*/
         /*----  sqllite seems to do an inner join when right table is filtered   ----*/

      3. wps proc python sql


StackOverflow R
https://stackoverflow.com/questions/76779753/illustrating-relative-timeline-in-r


options validvarname=upcase;
libname sd1 "d:/sd1";
data sd1.have;
  informat State $2. Filter $8. Threshold $7.;
  input State Filter Threshold;
cards4;
NJ Filter Exceeds
NJ Filter Exceeds
PA NoFilter .
NJ Filter NL
TX Filter Exceeds
;;;;;
run;quit;

/**************************************************************************************************************************/
/*                                                                                                                        */
/*  _                   _                                                                                                 */
/* (_)_ __  _ __  _   _| |_                                                                                               */
/* | | `_ \| `_ \| | | | __|                                                                                              */
/* | | | | | |_) | |_| | |_                                                                                               */
/* |_|_| |_| .__/ \__,_|\__|                                                                                              */
/*         |_|                                                                                                            */
/*                                                                                                                        */
/*                                                                                                                        */
/*  SD1.HAVE total obs=5                                                                                                  */
/*                                                                                                                        */
/*  Obs    STATE     FILTER     THRESHOLD                                                                                 */
/*                                                                                                                        */
/*   1      NJ      Filter       Exceeds                                                                                  */
/*   2      NJ      Filter       Exceeds                                                                                  */
/*   3      PA      NoFilter                                                                                              */
/*   4      NJ      Filter       NL                                                                                       */
/*   5      TX      Filter       Exceeds                                                                                  */
/*                                                                                                                        */
/*             _                                                                                                          */
/*  _ __ _   _| | ___  ___                                                                                                */
/* | `__| | | | |/ _ \/ __|                                                                                               */
/* | |  | |_| | |  __/\__ \                                                                                               */
/* |_|   \__,_|_|\___||___/                                                                                               */
/*                                                                                                                        */
/*                                                                                                                        */
/*                                                                                                                        */
/*  Obs STATE   FILTER      THRESHOLD    COUNT        |   COUNT                                                           */
/*                                       EXCEEDS      |   EXCEEDS                                                         */
/*                                       THRESHOLD    |   THRESHOLD                                                       */
/*                                                    |                                                                   */
/*                                                    |   count state filter combos where threshold=Exceeds               */
/*   1   NJ    Filter        Exceeds        2         |   2 with NJ and Filter and threshold=Exceeds                      */
/*   2   NJ    Filter        Exceeds        2         |   2 with NJ and Filter and threshold=Exceeds                      */
/*   3   PA    No Filter                    0         |   0 no count because we only count State Filter(not NL)           */
/*   4   NJ    Filter        NL             2         |   2 because matches on NJ and Filter but not threshold            */
/*   5   TX    Filter        Exceeds        1         |   1 TX Filter is unique                                           */
/*                                                              |                                                         */
/*                                        COUNT_TOTAL |   COUNT_TOTAL                                                     */
/*                                        TOTAL       |   TOTAL                                                           */
/*                                                    |                                                                   */
/*                                                    |                                                                   */
/*   1   NJ    Filter        Exceeds        3         |   3  count state filter combinations  NJ x Filter =3              */
/*   2   NJ    Filter        Exceeds        3         |   3  count state filter combinations  NJ x Filter =3              */
/*   3   PA    No Filter                    1         |   1  only 1 PA x No Filter                                        */
/*   4   NJ    Filter        NL             3         |   3  count state filter combinations  NJ x Filter =3              */
/*   5   TX    Filter        Exceeds        1         |   1  only 1 TX x Filter                                           */
/*                                                    |                                                                   */
/*                                                                                                                        */
/*              _               _                                                                                         */
/*   ___  _   _| |_ _ __  _   _| |_                                                                                       */
/*  / _ \| | | | __| `_ \| | | | __|                                                                                      */
/* | (_) | |_| | |_| |_) | |_| | |_                                                                                       */
/*  \___/ \__,_|\__| .__/ \__,_|\__|                                                                                      */
/*                 |_|                                                                                                    */
/*                                                                                                                        */
/* Obs    STATE     FILTER     THRESHOLD    COUNT_EXCEEDS_THRESHOLD    COUNT_TOTAL                                        */
/*                                                                                                                        */
/*  1      NJ      Filter       Exceeds                2                    3                                             */
/*  2      NJ      Filter       Exceeds                2                    3                                             */
/*  3      NJ      Filter       NL                     2                    3                                             */
/*  4      PA      NoFilter                            0                    1                                             */
/*  5      TX      Filter       Exceeds                1                    1                                             */
/*                                                       |                                                                */
/**************************************************************************************************************************/
   \
/*                                  _
/ | __      ___ __  ___   ___  __ _| |
| | \ \ /\ / / `_ \/ __| / __|/ _` | |
| |  \ V  V /| |_) \__ \ \__ \ (_| | |
|_|   \_/\_/ | .__/|___/ |___/\__, |_|
             |_|                 |_|
*/


%utl_submit_wps64('

libname sd1 "d:/sd1";
proc datasets lib=sd1 nolist nodetails;delete want; run;quit;

options missing="0" validvarname=any;;
proc sql;
  create
     table sd1.want as
  select
    state
   ,filter
   ,threshold
   ,count(state||filter) as count_total
   ,sum(case when Threshold = "Exceeds" then 1 else 0 end) as count_exceeds_threshold
  from
    sd1.have
  group
     by state, filter
;quit;
proc print data=sd1.want;
run;quit;
');

/**************************************************************************************************************************/
/*                                                                                                                        */
/* The WPS System                                                                                                         */
/*                                                                                                                        */
/* Obs    STATE     FILTER     THRESHOLD    COUNT_EXCEEDS_THRESHOLD    COUNT_TOTAL                                        */
/*                                                                                                                        */
/*  1      NJ      Filter       Exceeds                2                    3                                             */
/*  2      NJ      Filter       Exceeds                2                    3                                             */
/*  3      NJ      Filter       NL                     2                    3                                             */
/*  4      PA      NoFilter                            0                    1                                             */
/*  5      TX      Filter       Exceeds                1                    1                                             */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*___                _   _
|___ \   _ __   __ _| |_(_)_   _____   _ __
  __) | | `_ \ / _` | __| \ \ / / _ \ | `__|
 / __/  | | | | (_| | |_| |\ V /  __/ | |
|_____| |_| |_|\__,_|\__|_| \_/ \___| |_|

*/
proc datasets lib=sd1 nolist nodetails;delete want; run;quit;

%utl_submit_wps64x('
libname sd1 "d:/sd1";
proc r;
export data=sd1.have r=have;
submit;
library(tidyverse);
have;
want<-have %>%
  group_by(STATE, FILTER) %>%
  add_count(wt = (THRESHOLD== "Exceeds"), name = "Count_Exceeds_Threshold") %>%
  add_count(name = "Count_Total") %>%
  ungroup();
want;
endsubmit;
import data=sd1.want r=want;
proc print data=sd1.want;
run;quit;
');

/**************************************************************************************************************************/
/*                                                                                                                        */
/*  The WPS System                                                                                                        */
/*                                                                                                                        */
/* R                                                                                                                      */
/*                                                                                                                        */
/*  # A tibble: 5 x 5                                                                                                     */
/*    STATE FILTER   THRESHOLD Count_Exceeds_Threshold Count_Total                                                        */
/*    <chr> <chr>    <chr>                       <int>       <int>                                                        */
/*  1 NJ    Filter   "Exceeds"                       2           3                                                        */
/*  2 NJ    Filter   "Exceeds"                       2           3                                                        */
/*  3 PA    NoFilter ""                              0           1                                                        */
/*  4 NJ    Filter   "NL"                            2           3                                                        */
/*  5 TX    Filter   "Exceeds"                       1           1                                                        */
/*                                                                                                                        */
/*  WPS/SAS                                                                                                               */
/*                                                                                                                        */
/*  Obs    STATE     FILTER     THRESHOLD    COUNT_EXCEEDS_THRESHOLD    COUNT_TOTAL                                       */
/*                                                                                                                        */
/*   1      NJ      Filter       Exceeds                2                    3                                            */
/*   2      NJ      Filter       Exceeds                2                    3                                            */
/*   3      PA      NoFilter                            0                    1                                            */
/*   4      NJ      Filter       NL                     2                    3                                            */
/*   5      TX      Filter       Exceeds                1                    1                                            */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*____                                                                _
|___ /  __      ___ __  ___   _ __  _ __ ___   ___   _ __   ___  __ _| |
  |_ \  \ \ /\ / / `_ \/ __| | `_ \| `__/ _ \ / __| | `__| / __|/ _` | |
 ___) |  \ V  V /| |_) \__ \ | |_) | | | (_) | (__  | |    \__ \ (_| | |
|____/    \_/\_/ | .__/|___/ | .__/|_|  \___/ \___| |_|    |___/\__, |_|
                 |_|         |_|                                   |_|
*/

proc datasets lib=sd1 nolist nodetails;delete want; run;quit;

/*----  It looks like the left join will not return all left rows        ----*/
/*----  when the right table has any subsetting                          ----*/
/*----  sqllite seems to do an inner join in this case                   ----*/

%utl_submit_wps64x('
libname sd1 "d:/sd1";
proc r;
export data=sd1.have r=have;
submit;
library(sqldf);
want <- sqldf("
 select
  l.*
 from
  (select
    state
   ,filter
   ,threshold
   ,count(state||filter) as count_total
   ,sum(case when Threshold = `Exceeds` then 1 else 0 end) as count_exceeds_threshold
  from
    have
  group
     by state, filter ) as l
  left outer join (select
     state
    ,filter
  from
     have ) as r
  on
          l.state  = r.state
     and  l.filter = r.filter
");
want;
endsubmit;
import data=sd1.want r=want;
proc print data=sd1.want;
run;quit;
');

/**************************************************************************************************************************/
/*                                                                                                                        */
/* R The WPS System                                                                                                       */
/*                                                                                                                        */
/*   state   filter threshold count_total count_exceeds_threshold                                                         */
/* 1    NJ   Filter   Exceeds           3                       2                                                         */
/* 2    NJ   Filter   Exceeds           3                       2                                                         */
/* 3    NJ   Filter   Exceeds           3                       2                                                         */
/* 4    PA NoFilter                     1                       0                                                         */
/* 5    TX   Filter   Exceeds           1                       1                                                         */
/*                                                                                                                        */
/* WPS                                                                                                                    */
/*                                                                                                                        */
/* Obs    STATE     FILTER     THRESHOLD    COUNT_TOTAL    COUNT_EXCEEDS_THRESHOLD                                        */
/*                                                                                                                        */
/*  1      NJ      Filter       Exceeds          3                    2                                                   */
/*  2      NJ      Filter       Exceeds          3                    2                                                   */
/*  3      NJ      Filter       Exceeds          3                    2                                                   */
/*  4      PA      NoFilter                      1                    0                                                   */
/*  5      TX      Filter       Exceeds          1                    1                                                   */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*  _                                      _   _                             _
| || |   __      ___ __  ___   _ __  _   _| |_| |__   ___  _ __    ___  __ _| |
| || |_  \ \ /\ / / `_ \/ __| | `_ \| | | | __| `_ \ / _ \| `_ \  / __|/ _` | |
|__   _|  \ V  V /| |_) \__ \ | |_) | |_| | |_| | | | (_) | | | | \__ \ (_| | |
   |_|     \_/\_/ | .__/|___/ | .__/ \__, |\__|_| |_|\___/|_| |_| |___/\__, |_|
                  |_|         |_|    |___/                                |_|
*/

%utl_submit_wps64x('

libname sd1 "d:/sd1";
proc datasets lib=sd1 nolist nodetails;delete want; run;quit;

proc python;
export data=sd1.have python=have;
submit;
 from os import path;
 import pandas as pd;
 import numpy as np;
 import pandas as pd;
 from pandasql import sqldf;
 mysql = lambda q: sqldf(q, globals());
 from pandasql import PandaSQL;
 pdsql = PandaSQL(persist=True);
 sqlite3conn = next(pdsql.conn.gen).connection.connection;
 sqlite3conn.enable_load_extension(True);
 sqlite3conn.load_extension("c:/temp/libsqlitefunctions.dll");
 mysql = lambda q: sqldf(q, globals());
 want=pdsql("""
 select
  l.*
 from
  (select
    state
   ,filter
   ,threshold
   ,count(state||filter) as count_total
   ,sum(case when trim(Threshold) = `Exceeds` then 1 else 0 end) as count_exceeds_threshold
  from
    have
  group
     by state, filter ) as l
  left join (select
     state
    ,filter
  from
     have ) as r
  on
          l.state  = r.state
     and  l.filter = r.filter
 """);
print(want);
endsubmit;
import data=sd1.want python=want;
run;quit;
proc print data=sd1.want;
run;quit;
');

/**************************************************************************************************************************/
/*                                                                                                                        */
/* Python The WPS System                                                                                                  */
/*                                                                                                                        */
/* The PYTHON Procedure                                                                                                   */
/*                                                                                                                        */
/*   state    filter threshold  count_total  count_exceeds_threshold                                                      */
/* 0    NJ  Filter     Exceeds            3                        2                                                      */
/* 1    NJ  Filter     Exceeds            3                        2                                                      */
/* 2    NJ  Filter     Exceeds            3                        2                                                      */
/* 3    PA  NoFilter                      1                        0                                                      */
/* 4    TX  Filter     Exceeds            1                        1                                                      */
/*                                                                                                                        */
/* WPS                                                                                                                    */
/*                                                                                                                        */
/* Obs    STATE     FILTER     THRESHOLD    COUNT_TOTAL    COUNT_EXCEEDS_THRESHOLD                                        */
/*                                                                                                                        */
/*  1      NJ      Filter       Exceeds          3                    2                                                   */
/*  2      NJ      Filter       Exceeds          3                    2                                                   */
/*  3      NJ      Filter       Exceeds          3                    2                                                   */
/*  4      PA      NoFilter                      1                    0                                                   */
/*  5      TX      Filter       Exceeds          1                    1                                                   */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*              _
  ___ _ __   __| |
 / _ \ `_ \ / _` |
|  __/ | | | (_| |
 \___|_| |_|\__,_|

*/
