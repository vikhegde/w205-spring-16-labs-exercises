Step by step instructions for running the application
======================================================

ASSUMPTIONS: These directions assume the following:
1. An EC2 instance as supplied to students via an AMI specifically to run Exercise 2 (with components like streamparse and postgres installed) is available
2. Postgresql is installed and password is "pass" for postgres user
3. A Python2.7 virtualenv (such as the one at /opt/py27environment supplied to students is available)
4. Storm, tweepy, streamparse and other modules required by sample code have been installed in a virtualenv

Step 0
======
Become a non-priveleged user with sudo priveleges

Step 1
======
Copy the entire directory EXtwotweetwordcount and contents in the Git repository to a directory (say /data) in the EC2 instance
Step 2
======
Activate the Python 2.7 virtualenv as follows (assumes virtualenv at /opt/py27environment)
$  source /opt/py27environment/bin/activate
Step 3
======
Start up postgres. This depends on how postgresql is installed on your system, but let us assume we use the default install in Centos/Red Hat linux
$  sudo /etc/init.d/postgresql start
Step 4
======
Check that the DB "tcount" and table "tweetwordcount" are present and accessible in postgresql. An easy way to do this is via psql
$ psql -U postgres
Password for user postgres:  <enter password "pass"  here>

postgres=# psql (8.4.20)
postgres-# Type "help" for help.
postgres-#

Connect to tcount DB

postgres-# \c tcount
psql (8.4.20)
You are now connected to database "tcount".
tcount-#

Check table tweetwordcount

tcount-#  \d tweetwordcount

     Table "public.tweetwordcount"
 Column |       Type        | Modifiers
--------+-------------------+-----------
 word   | character varying | not null
 count  | integer           |
Indexes:
    "pkey_word" PRIMARY KEY, btree (word)

tcount-#

Quit psql
tcount-# \q

Step 5
======
Start application by cd'ing to EXtwotweetwordcount top level directory

Step 6
======
Issue the following command

$ sparse run

The application takes some time to start and emits lots of logging messages as it starts. Once it has started running a scrolling display of:
    each word, its count since app was started, its stored count (in the DB) which is same as or greater than count since app was started

    Here is a sample line:

    65261 [Thread-29] INFO  backtype.storm.task.ShellBolt - ShellLog pid:12528, name:count-bolt OK: I: 18, 239

  The word that follows OK is the "word" and the two counts after that are the a) count since app was started b) count stored in the DB (over multiple runs of the app.) The stored count is always equal to or greater than the other count.)

  To terminate the application, hit Ctrl-C

Step 7
=======
Run finalresults.py to get word count for a specific word or for all words in the DB (NOTE: This is stored count)

$ finalresults.py is

This gives the stored count for the word "is"

$ ./finalresults.py

This gives a listing of all words in the DB and their corresponding stored counts

Step 8
=======
Run histogram.py to get "stored" words whose counts fall in an interval

For example:

$ ./histogram.py 2,10

gives the set of all stored words whose stored count is between 2 and 10 (both limits inclusive)

IMPORTANT NOTE: The interval (say 2,10) must be specified as a single argument (no space between 2 and 10) and 2 and 10 must be separated by a comma.
Otherwise the histogram.py script will terminate with a "usage" message.