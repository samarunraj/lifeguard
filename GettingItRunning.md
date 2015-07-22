This page describes how to run the various pieces of the application.

Lifeguard is packaged with all dependency libraries, which is why the zip file is so large.
It comes with API docs, scripts and config files. If I've forgotten anything, please contact me, post in the forum or create an issue.

### Pool Manager ###
The pool manager can be run from the command-line. The script "poolManager.sh" in the base install directory will run it. You should edit the conf/aws.properties file to set your amazon access id and secret key. The queue prefix is used to allow you to run several pool managers within one user account. I normally set this to my initials. You should edit the conf/poolconfig.xml to set the services you'll support. The file that comes with the distribution only enables the TextToPdf service. There is a public AMI already available for that service.

### Ingestor ###
The ingestor implementation that is provided in this release takes a list of files as arguments. To run it, use the "ingest.sh" script in the base install directory. You must provide some required arguments before supplying the files.
```
./ingest.sh Project batch001 myBucket conf/sampleWorkflow.xml <file 1> ... <file n>
```

### Service ###
There is an AMI (ami-30e40159) which implements the TextToPDF service. While you can run a service on your own computer, I recommend running them at Amazon. The service base class attempts to pull the instance ID from instance meta-data and that will eventually timeout on your own machine (using "unknown" for instance ID). To start the instance your self, you should supply the same user data that the pool manager supplies (which makes testing easier).
```
ec2run ami-30e40159 -d "<accessId> <secretKey> <queuePrefix> <serviceName>" -k your-keypair
```
For serviceName, you can supply "TextToPDF".

Once you login, type "su - lifeguard" since that is the user the service runs under. What you see in the /home/lifeguard directory is basically the lifeguard 0.1 release with a modified run\_service.sh script. The script passes the user data into the executable.